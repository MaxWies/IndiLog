#pragma once

#include "log/index.h"
#include "log/utils.h"

namespace faas {
namespace log {

struct IndexReadOp {
    absl::flat_hash_set<uint16_t> aggregated_nodes;
    IndexQueryResult index_query_result;
};

typedef tbb::concurrent_hash_map<uint64_t, IndexReadOp*> OngoingIndexReadsTable;

class EngineIndexReadOp {
public:
    EngineIndexReadOp();
    ~EngineIndexReadOp();
    bool Aggregate(size_t num_index_shards, uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* aggregated_index_query_result);
private:
    std::string log_header_;
    OngoingIndexReadsTable ongoing_index_reads_;

    DISALLOW_COPY_AND_ASSIGN(EngineIndexReadOp);
};

}  // namespace log
}  // namespace faas
