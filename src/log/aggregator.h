#pragma once

#include "log/aggregator_base.h"
#include "log/aggregating.h"
#include "log/log_space.h"
#include "log/index_dto.h"
#include "log/utils.h"

namespace faas {
namespace log {

class Aggregator final : public AggregatorBase {
public:
    explicit Aggregator(uint16_t node_id);
    ~Aggregator();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_      ABSL_GUARDED_BY(view_mu_);
    bool current_view_active_        ABSL_GUARDED_BY(view_mu_);
    std::vector<const View*> views_  ABSL_GUARDED_BY(view_mu_);

    absl::flat_hash_map</*engine_id*/uint16_t, std::unique_ptr<EngineIndexReadOp>> ongoing_engine_index_reads_ ABSL_GUARDED_BY(view_mu_);


    void OnViewCreated(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void OnRecvRegistration(const protocol::SharedLogMessage& message) override;
    void RemoveEngineNode(uint16_t engine_node_id) override;


    void HandleSlaveResult(const protocol::SharedLogMessage& message) override;
    bool AggregateIndexResult(const uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* aggregated_index_query_result);

    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void ForwardReadRequest(const IndexQueryResult& query_result);

    protocol::SharedLogMessage BuildReadRequestMessage(const IndexQueryResult& result);

    IndexQuery BuildIndexQuery(const protocol::SharedLogMessage& message, const uint16_t original_requester_id);
    IndexQuery BuildIndexQuery(const IndexQueryResult& result);

    IndexQueryResult BuildIndexResult(protocol::SharedLogMessage message);

    DISALLOW_COPY_AND_ASSIGN(Aggregator);
};

}  // namespace log
}  // namespace faas
