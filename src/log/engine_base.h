#pragma once

#include "common/zk.h"
#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "log/index_dto.h"
#include "log/cache.h"
#include "server/io_worker.h"
#include "utils/object_pool.h"
#include "utils/appendable_buffer.h"

namespace faas {

// Forward declaration
namespace engine { class Engine; }

namespace log {

class EngineBase {
public:
    explicit EngineBase(engine::Engine* engine);
    virtual ~EngineBase();

    void Start();
    void Stop();

    void OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);

    void OnNewExternalFuncCall(const protocol::FuncCall& func_call, uint32_t log_space);
    void OnNewInternalFuncCall(const protocol::FuncCall& func_call,
                               const protocol::FuncCall& parent_func_call);
    void OnFuncCallCompleted(const protocol::FuncCall& func_call);
    void OnMessageFromFuncWorker(const protocol::Message& message);

protected:
    uint16_t my_node_id() const { return node_id_; }
    zk::ZKSession* zk_session();

    virtual void OnViewCreated(const View* view) = 0;
    virtual void OnViewFrozen(const View* view) = 0;
    virtual void OnViewFinalized(const FinalizedView* finalized_view) = 0;

    virtual void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                                   std::span<const char> payload) = 0;
    virtual void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                                    std::span<const char> payload) = 0;
    virtual void OnRecvResponse(const protocol::SharedLogMessage& message,
                                std::span<const char> payload) = 0;
    virtual void OnRecvRegistrationResponse(const protocol::SharedLogMessage& message) = 0;

    void MessageHandler(const protocol::SharedLogMessage& message,
                        std::span<const char> payload);

    struct LocalOp {
        protocol::SharedLogOpType type;
        uint16_t client_id;
        uint32_t user_logspace;
        uint64_t id;
        uint64_t client_data;
        uint64_t metalog_progress;
        uint64_t query_tag;
        uint64_t seqnum;
        uint64_t func_call_id;
        int64_t start_timestamp;
        bool index_lookup_miss;
        UserTagVec user_tags;
        utils::AppendableBuffer data;
    };

    virtual void HandleLocalAppend(LocalOp* op) = 0;
    virtual void HandleLocalTrim(LocalOp* op) = 0;
    virtual void HandleLocalRead(LocalOp* op) = 0;
    virtual void HandleLocalSetAuxData(LocalOp* op) = 0;

    void LocalOpHandler(LocalOp* op);

    void ReplicateLogEntry(const View* view, const View::StorageShard* storage_shard, const LogMetaData& log_metadata,
                           std::span<const uint64_t> user_tags,
                           std::span<const char> log_data);
    void PropagateAuxData(const View* view, const View::StorageShard* storage_shard, const LogMetaData& log_metadata, 
                          std::span<const char> aux_data);

    void FinishLocalOpWithResponse(LocalOp* op, protocol::Message* response,
                                   uint64_t metalog_progress, bool success = true);
    void FinishLocalOpWithFailure(LocalOp* op, protocol::SharedLogResultType result,
                                  uint64_t metalog_progress = 0);

    void LogCachePut(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
                     std::span<const char> log_data);
    std::optional<LogEntry> LogCacheGet(uint64_t seqnum);
    void LogCachePutAuxData(uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> LogCacheGetAuxData(uint64_t seqnum);

    bool SendIndexTierReadRequest(uint16_t index_node_id, protocol::SharedLogMessage* request);
    bool SendStorageReadRequest(const IndexQueryResult& result, const View::StorageShard* storage_shard);
    void SendReadResponse(const IndexQuery& query,
                          protocol::SharedLogMessage* response,
                          std::span<const char> user_tags_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> data_payload = EMPTY_CHAR_SPAN,
                          std::span<const char> aux_data_payload = EMPTY_CHAR_SPAN);
    void SendReadFailureResponse(const IndexQuery& query,
                                 protocol::SharedLogResultType result_type,
                                 uint64_t metalog_progress = 0);
    bool SendSequencerMessage(uint16_t sequencer_id,
                              protocol::SharedLogMessage* message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN);

    bool SendRegistrationRequest(uint16_t destination_id, protocol::ConnType connection_type, protocol::SharedLogMessage* message);

    void OnActivationZNodeCreated(std::string_view path, std::span<const char> contents);
    virtual void OnActivateCaching() = 0;
    void SetMissedView(const View* view) {
        missed_view_ = view;
    }

    bool postpone_registration(){
        absl::ReaderMutexLock fn_ctx_lk(&fn_ctx_mu_);
        return postpone_registration_;
    }
    bool postpone_caching(){
        absl::ReaderMutexLock fn_ctx_lk(&fn_ctx_mu_);
        return postpone_caching_;
    }

    void registered(){
        absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
        postpone_registration_ = false;
        registered_ = true;
    }

    server::IOWorker* SomeIOWorker();

#ifdef __FAAS_STAT_THREAD
    std::optional<zk_utils::DirWatcher> statistics_watcher_;
    uint64_t previous_local_op_id_;
    uint64_t LoadLocalOpId(){
        return next_local_op_id_.load();
    }
    void OnStatZNodeCreated(std::string_view path, std::span<const char> contents);
    virtual void OnActivateStatisticsThread(int statistic_thread_interval_sec) = 0;
#endif

#ifdef __FAAS_OP_LATENCY
struct OpLatency{
    protocol::SharedLogOpType type;
    int64_t duration;
    bool success;
};
    std::vector<OpLatency> finished_operations_ ABSL_GUARDED_BY(fn_ctx_mu_);
    void ResetOpLatencies(){
        absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
        finished_operations_.clear();
    }
    void PrintOpLatencies(std::ostringstream* append_results, std::ostringstream* read_results);
#endif
#ifdef __FAAS_OP_TRACING
struct OpTrace{
    protocol::SharedLogOpType type;
    std::vector<int64_t> absolute_ts;
    std::vector<int64_t> relative_ts;
    std::vector<std::string> func_desc;
};

    absl::Mutex trace_mu_;
    const uint32_t trace_granularity_ = 1000;
    absl::flat_hash_map<uint64_t, std::unique_ptr<OpTrace>> traces_ ABSL_GUARDED_BY(trace_mu_);
    absl::flat_hash_set<uint64_t> finished_traces_ ABSL_GUARDED_BY(trace_mu_);

    bool IsOpTraced(uint64_t id);
    void InitTrace(uint64_t id, protocol::SharedLogOpType type, int64_t first_ts, const std::string func_desc);
    void SaveTracePoint(uint64_t id, const std::string func_desc);
    void SaveOrIncreaseTracePoint(uint64_t id, const std::string func_desc);
    void CompleteTrace(uint64_t id, const std::string func_desc);
    void PrintTrace(std::ostringstream* append_results, std::ostringstream* read_results, const OpTrace* op_trace);
#endif

private:
    const uint16_t node_id_;
    engine::Engine* engine_;

    ViewWatcher view_watcher_;
    const View* missed_view_;

    std::optional<zk_utils::DirWatcher> activation_watcher_;

    utils::ThreadSafeObjectPool<LocalOp> log_op_pool_;
    std::atomic<uint64_t> next_local_op_id_;

    struct FnCallContext {
        uint32_t user_logspace;
        uint64_t metalog_progress;
        uint64_t parent_call_id;
    };

    absl::Mutex fn_ctx_mu_;
    absl::flat_hash_map</* full_call_id */ uint64_t, FnCallContext>
        fn_call_ctx_ ABSL_GUARDED_BY(fn_ctx_mu_);

    bool registered_ ABSL_GUARDED_BY(fn_ctx_mu_);
    bool postpone_registration_ ABSL_GUARDED_BY(fn_ctx_mu_);
    bool postpone_caching_ ABSL_GUARDED_BY(fn_ctx_mu_);

    std::optional<LRUCache> log_cache_;

    void SetupZKWatchers();
    void SetupTimers();

    void PopulateLogTagsAndData(const protocol::Message& message, LocalOp* op);

    DISALLOW_COPY_AND_ASSIGN(EngineBase);
};

}  // namespace log
}  // namespace faas
