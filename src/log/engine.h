#pragma once

#include "log/engine_base.h"
#include "log/log_space.h"
#include "log/index.h"
#include "log/index_local_suffix.h"
#include "log/index_local_seq_cache.h"
#include "log/index_local_tag_cache.h"
#include "log/utils.h"

namespace faas {

// Forward declaration
namespace engine { class Engine; }

namespace log {

enum class IndexingStrategy {
    COMPLETE,
    DISTRIBUTED,
    INDEX_TIER_ONLY
};

class Engine final : public EngineBase {
public:
    explicit Engine(engine::Engine* engine);
    ~Engine();

private:
    std::string log_header_;
    IndexingStrategy indexing_strategy_;

    absl::Mutex view_mu_;
    const View* current_view_          ABSL_GUARDED_BY(view_mu_);
    ViewMutable view_mutable_       ABSL_GUARDED_BY(view_mu_);
    bool current_view_active_        ABSL_GUARDED_BY(view_mu_);
    std::vector<const View*> views_  ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<LogProducer>
        producer_collection_         ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<Index>
        index_collection_            ABSL_GUARDED_BY(view_mu_);

    // distributed indexing
    PhysicalLogSpaceCollection<SeqnumSuffixChain> suffix_chain_collection_ ABSL_GUARDED_BY(view_mu_);
    PhysicalLogSpaceCollection<TagCache> tag_cache_collection_ ABSL_GUARDED_BY(view_mu_);
    std::optional<SeqnumCache> seqnum_cache_;

    log_utils::FutureRequests       future_requests_;
    log_utils::ThreadedMap<LocalOp> onging_reads_;

#ifdef __FAAS_STAT_THREAD
    base::Thread statistics_thread_;
    bool statistics_thread_started_;
    uint64_t previous_total_ops_counter_;
#endif

#ifdef __FAAS_OP_STAT
    std::atomic<uint64_t> append_ops_counter_;
    std::atomic<uint64_t> read_ops_counter_;
    std::atomic<uint64_t> local_index_hit_counter_;
    std::atomic<uint64_t> local_index_miss_counter_;
    std::atomic<uint64_t> index_min_read_ops_counter_;
    std::atomic<uint64_t> log_cache_hit_counter_;
    std::atomic<uint64_t> log_cache_miss_counter_;
#endif

    void OnViewCreated(const View* view) override;
    void OnViewFrozen(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleLocalAppend(LocalOp* op) override;
    void HandleLocalTrim(LocalOp* op) override;
    void HandleLocalRead(LocalOp* op) override;
    void HandleLocalSetAuxData(LocalOp* op) override;

    void HandleIndexTierRead(LocalOp* op, uint16_t view_id, const View::StorageShard* storage_shard);
    void HandleIndexTierMinSeqnumRead(LocalOp* op, uint64_t tag, uint16_t view_id, uint64_t log_tail_seqnum, const View::StorageShard* storage_shard);
    void ProcessLocalIndexMisses(const Index::QueryResultVec& miss_results, uint32_t logspace_id);

    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;
    void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                            std::span<const char> payload) override;
    void OnRecvResponse(const protocol::SharedLogMessage& message,
                        std::span<const char> payload) override;
    void OnRecvRegistrationResponse(const protocol::SharedLogMessage& message) override;

    void ProcessAppendResults(const LogProducer::AppendResultVec& results);
    void ProcessIndexQueryResults(const Index::QueryResultVec& results, Index::QueryResultVec* not_found_results);
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
    protocol::SharedLogMessage BuildIndexTierReadRequestMessage(LocalOp* op, uint16_t master_node_id);
    protocol::SharedLogMessage BuildIndexTierMinSeqnumRequestMessage(LocalOp* op, uint64_t tag, uint64_t log_tail_seqnum);
    protocol::SharedLogMessage BuildReadRequestMessage(const IndexQueryResult& result);

    IndexQuery BuildIndexQuery(LocalOp* op);
    IndexQuery BuildIndexTierQuery(LocalOp* op, uint16_t master_node_id);
    IndexQuery BuildIndexQuery(const protocol::SharedLogMessage& message);
    IndexQuery BuildIndexQuery(const IndexQueryResult& result);

#ifdef __FAAS_STAT_THREAD
    void StatisticsThreadMain();        
#endif

    DISALLOW_COPY_AND_ASSIGN(Engine);
};

}  // namespace log
}  // namespace faas
