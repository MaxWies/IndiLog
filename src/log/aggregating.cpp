#include "log/aggregating.h"

#include "log/flags.h"
#include "log/utils.h"
#include "utils/bits.h"
#include "utils/io.h"
#include "utils/timerfd.h"

namespace faas {
namespace log {

EngineIndexReadOp::EngineIndexReadOp(){
    log_header_ = "EngineIndexReadOp";
}

EngineIndexReadOp::~EngineIndexReadOp(){}

bool EngineIndexReadOp::Merge(size_t num_index_shards, uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* merged_index_query_result){
    const uint64_t key = index_query_result_other.original_query.client_data;
    OngoingIndexReadsTable::accessor accessor;
    if (ongoing_index_reads_.insert(accessor, key)) {
        // HVLOG_F(1, "IndexRead: Create new index read operation for key={}", bits::HexStr0x(key));
        accessor->second = new IndexReadOp();
        accessor->second->merged_nodes.insert(index_node_id_other);
        accessor->second->index_query_result = index_query_result_other;
    } else {
        HVLOG_F(1, "IndexRead: Retrieve index read operation for key={}", bits::HexStr0x(key));
        uint64_t mergedResult = accessor->second->index_query_result.found_result.seqnum;
        uint64_t otherResult = index_query_result_other.found_result.seqnum;
        HVLOG_F(1, "IndexRead: Merging: merged_result={}, other_result={}", mergedResult, otherResult);
        if (accessor->second->index_query_result.state == IndexQueryResult::kFound && index_query_result_other.state == IndexQueryResult::kEmpty){
            // merged result is found, other result is empty
            HVLOG (1) << "IndexRead: Current result is FOUND, other result is EMPTY";
        } 
        else if (accessor->second->index_query_result.state == IndexQueryResult::kEmpty && index_query_result_other.state == IndexQueryResult::kFound) {
            // merged result is empty, other result is found
            HVLOG (1) << "IndexRead: Current result is EMPTY, other result is FOUND";
            accessor->second->index_query_result = index_query_result_other;
        }
        else if (accessor->second->index_query_result.original_query.direction == IndexQuery::ReadDirection::kReadPrev){
            if (mergedResult < otherResult) {
                // other result is closer
                HVLOG_F(1, "IndexRead: Current result is FOUND({}), other result is FOUND({}). Other is closer for read_prev and query_seqnum={}", 
                    mergedResult, otherResult, bits::HexStr0x(accessor->second->index_query_result.original_query.query_seqnum)
                );
                accessor->second->index_query_result = index_query_result_other;
            }
        } 
        else { // readNext, readNextB
            if (otherResult < mergedResult) {
                // other result is closer
                HVLOG_F(1, "IndexRead: Current result is FOUND({}), other result is FOUND({}). Other is closer for read_next and query_seqnum={}", 
                    mergedResult, otherResult, bits::HexStr0x(accessor->second->index_query_result.original_query.query_seqnum)
                );
                accessor->second->index_query_result = index_query_result_other;
            }
        }
        accessor->second->merged_nodes.insert(index_node_id_other);
        HVLOG_F(1, "IndexRead: Merged {} results. key={}", accessor->second->merged_nodes.size(), bits::HexStr0x(key));
    }
    if(accessor->second->merged_nodes.size() == num_index_shards){
        *merged_index_query_result = accessor->second->index_query_result;
        accessor.release();
        ongoing_index_reads_.erase(key);
        return true;
    }
    return false;
}

}  // namespace log
}  // namespace faas
