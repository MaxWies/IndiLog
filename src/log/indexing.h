#pragma once

#include "log/indexing_base.h"
#include "log/log_space.h"
#include "log/index.h"
#include "log/utils.h"

namespace faas {
namespace log {

class IndexNode final : public IndexBase {
public:
    explicit IndexNode(uint16_t node_id);
    ~IndexNode();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_      ABSL_GUARDED_BY(view_mu_);
    bool current_view_active_        ABSL_GUARDED_BY(view_mu_);
    std::vector<const View*> views_  ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<Index>
        index_collection_        ABSL_GUARDED_BY(view_mu_);

    absl::Mutex index_reads_mu_;
    utils::ThreadSafeObjectPool<IndexReadOp> index_read_op_pool_;
    std::atomic<uint64_t> next_index_read_op_id_;
    log_utils::FutureRequests future_requests_;
    absl::flat_hash_map<std::pair</*engine_id*/uint16_t, /* client_data */ uint64_t>, IndexReadOp*> ongoing_index_reads_ ABSL_GUARDED_BY(index_reads_mu_);

    void OnViewCreated(const View* view) override;
    // void OnViewFrozen(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleReadRequest(const protocol::SharedLogMessage& request) override;
    void HandleReadMinRequest(const protocol::SharedLogMessage& request) override;
    void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                            std::span<const char> payload) override;

    bool MergeIndexResult(const uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* merged_index_query_result);
    void HandleSlaveResult(const protocol::SharedLogMessage& message, std::span<const char> payload) override;

    void ProcessIndexQueryResults(const Index::QueryResultVec& results);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void ProcessIndexResult(const IndexQueryResult& query_result);
    void ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                    Index::QueryResultVec* more_results);

    void ForwardReadRequest(const IndexQueryResult& query_result);

    //void BackgroundThreadMain() override;
    
    void FlushIndexEntries();

    protocol::SharedLogMessage BuildReadRequestMessage(const IndexQueryResult& result);

    IndexQuery BuildIndexQuery(const protocol::SharedLogMessage& message, const uint16_t original_requester_id);
    IndexQuery BuildIndexQuery(const IndexQueryResult& result);

    IndexQueryResult BuildIndexResult(protocol::SharedLogMessage message, IndexResultProto result);

    DISALLOW_COPY_AND_ASSIGN(IndexNode);
};

}  // namespace log
}  // namespace faas
