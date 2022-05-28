#pragma once

#include "log/engine_base.h"
#include "log/log_space.h"
#include "log/index.h"
#include "log/utils.h"

namespace faas {

// Forward declaration
namespace engine { class Engine; }

namespace log {

class Engine final : public EngineBase {
public:
    explicit Engine(engine::Engine* engine);
    ~Engine();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_        ABSL_GUARDED_BY(view_mu_);
    bool current_view_active_        ABSL_GUARDED_BY(view_mu_);
    std::vector<const View*> views_  ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<LogProducer>
        producer_collection_         ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<Index>
        index_collection_            ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests       future_requests_;
    log_utils::ThreadedMap<LocalOp> onging_reads_;

#ifdef __FAAS_STAT_THREAD
    base::Thread statistics_thread_;
    bool statistics_thread_started_;
    int statistic_thread_interval_sec_;
    uint64_t previous_total_ops_counter_;
    void OnActivateStatisticsThread(int statistic_thread_interval_sec) override;
    void StatisticsThreadMain();
#endif

#ifdef __FAAS_OP_STAT
    std::atomic<uint64_t> append_ops_counter_;
    std::atomic<uint64_t> read_ops_counter_;
    std::atomic<uint64_t> local_index_hit_counter_;
    std::atomic<uint64_t> local_index_miss_counter_;
    std::atomic<uint64_t> index_min_read_ops_counter_; // has no significance
    std::atomic<uint64_t> log_cache_hit_counter_;
    std::atomic<uint64_t> log_cache_miss_counter_;
    void ResetOpStat(){
        append_ops_counter_.store(0);
        read_ops_counter_.store(0);
        local_index_hit_counter_.store(0);
        local_index_miss_counter_.store(0);
        index_min_read_ops_counter_.store(0);
        log_cache_hit_counter_.store(0);
        log_cache_miss_counter_.store(0);
    }
#endif

    void OnViewCreated(const View* view) override;
    void OnViewFrozen(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleLocalAppend(LocalOp* op) override;
    void HandleLocalTrim(LocalOp* op) override;
    void HandleLocalRead(LocalOp* op) override;
    void HandleLocalSetAuxData(LocalOp* op) override;

    void HandleRemoteRead(const protocol::SharedLogMessage& request) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;
    void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                            std::span<const char> payload) override;
    void OnRecvResponse(const protocol::SharedLogMessage& message,
                        std::span<const char> payload) override;

    void ProcessAppendResults(const LogProducer::AppendResultVec& results);
    void ProcessIndexQueryResults(const Index::QueryResultVec& results);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void ProcessIndexFoundResult(const IndexQueryResult& query_result);
    void ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                    Index::QueryResultVec* more_results);

    inline LogMetaData MetaDataFromAppendOp(LocalOp* op) {
        DCHECK(op->type == protocol::SharedLogOpType::APPEND);
        return LogMetaData {
            .user_logspace = op->user_logspace,
            .seqnum = kInvalidLogSeqNum,
            .localid = 0,
            .num_tags = op->user_tags.size(),
            .data_size = op->data.length()
        };
    }

    protocol::SharedLogMessage BuildReadRequestMessage(LocalOp* op);
    protocol::SharedLogMessage BuildReadRequestMessage(const IndexQueryResult& result);

    IndexQuery BuildIndexQuery(LocalOp* op);
    IndexQuery BuildIndexQuery(const protocol::SharedLogMessage& message);
    IndexQuery BuildIndexQuery(const IndexQueryResult& result);

    DISALLOW_COPY_AND_ASSIGN(Engine);
};

}  // namespace log
}  // namespace faas
