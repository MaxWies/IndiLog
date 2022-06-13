#pragma once

#include "log/merger_base.h"
#include "log/log_space.h"
#include "log/index.h"
#include "log/utils.h"

namespace faas {
namespace log {

struct IndexReadOp {
    absl::flat_hash_set<uint16_t> merged_nodes;
    IndexQueryResult index_query_result;
};

typedef tbb::concurrent_hash_map<uint64_t, IndexReadOp*> OngoingIndexReadsTable;

class EngineIndexReadOp {
public:
    EngineIndexReadOp();
    ~EngineIndexReadOp();
    bool Merge(size_t num_index_shards, uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* merged_index_query_result);
private:
    std::string log_header_;
    OngoingIndexReadsTable ongoing_index_reads_;
};

class Merger final : public MergerBase {
public:
    explicit Merger(uint16_t node_id);
    ~Merger();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_      ABSL_GUARDED_BY(view_mu_);
    ViewMutable view_mutable_      ABSL_GUARDED_BY(view_mu_);
    bool current_view_active_        ABSL_GUARDED_BY(view_mu_);
    std::vector<const View*> views_  ABSL_GUARDED_BY(view_mu_);

    absl::flat_hash_map</*engine_id*/uint16_t, std::unique_ptr<EngineIndexReadOp>> ongoing_engine_index_reads_ ABSL_GUARDED_BY(view_mu_);


    void OnViewCreated(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void OnRecvRegistration(const protocol::SharedLogMessage& message) override;
    void RemoveEngineNode(uint16_t engine_node_id) override;


    void HandleSlaveResult(const protocol::SharedLogMessage& message) override;
    bool MergeIndexResult(const uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* merged_index_query_result);

    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void ForwardReadRequest(const IndexQueryResult& query_result);

    protocol::SharedLogMessage BuildReadRequestMessage(const IndexQueryResult& result);

    IndexQuery BuildIndexQuery(const protocol::SharedLogMessage& message, const uint16_t original_requester_id);
    IndexQuery BuildIndexQuery(const IndexQueryResult& result);

    IndexQueryResult BuildIndexResult(protocol::SharedLogMessage message);

    DISALLOW_COPY_AND_ASSIGN(Merger);
};

}  // namespace log
}  // namespace faas
