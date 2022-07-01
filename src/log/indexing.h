#pragma once

#include "log/indexing_base.h"
#include "log/log_space.h"
#include "log/index.h"
#include "log/aggregating.h"
#include "log/utils.h"

namespace faas {
namespace log {

struct PerTagMinSeqnum {
    uint64_t seqnum;
    uint16_t storage_shard_id;
};

typedef tbb::concurrent_hash_map<uint64_t, PerTagMinSeqnum> PerTagMinSeqnumTable;

class IndexNode final : public IndexBase {
public:
    explicit IndexNode(uint16_t node_id);
    ~IndexNode();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_        ABSL_GUARDED_BY(view_mu_);
    ViewMutable view_mutable_        ABSL_GUARDED_BY(view_mu_);
    bool current_view_active_        ABSL_GUARDED_BY(view_mu_);
    std::vector<const View*> views_  ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<Index>
        index_collection_            ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests future_requests_;

    absl::flat_hash_map</*engine_id*/uint16_t, std::unique_ptr<EngineIndexReadOp>> ongoing_engine_index_reads_ ABSL_GUARDED_BY(view_mu_);

    bool per_tag_seqnum_min_completion_;
    PerTagMinSeqnumTable per_tag_min_seqnum_table_;

    void OnViewCreated(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleReadRequest(const protocol::SharedLogMessage& request) override;
    void HandleReadMinRequest(const protocol::SharedLogMessage& request) override;
    void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                            std::span<const char> payload) override;
    void FilterNewTags(const View* view, uint32_t logspace_id, const IndexDataProto& index_data_proto);
    void OnRecvRegistration(const protocol::SharedLogMessage& message) override;

    void RemoveEngineNode(uint16_t engine_node_id) override;

    void ProcessIndexQueryResults(const Index::QueryResultVec& results);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void HandleSlaveResult(const protocol::SharedLogMessage& message) override;
    bool AggregateIndexResult(const uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* aggregated_index_query_result);

    void ProcessIndexResult(const IndexQueryResult& query_result);
    void ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                    Index::QueryResultVec* more_results);

    void ForwardReadRequest(const IndexQueryResult& query_result);
   
    void FlushIndexEntries();

    protocol::SharedLogMessage BuildReadRequestMessage(const IndexQueryResult& result);

    IndexQuery BuildIndexQuery(const protocol::SharedLogMessage& message, const uint16_t original_requester_id);
    IndexQuery BuildIndexQuery(const IndexQueryResult& result);
    IndexQueryResult BuildIndexResult(protocol::SharedLogMessage message);

    DISALLOW_COPY_AND_ASSIGN(IndexNode);
};

}  // namespace log
}  // namespace faas
