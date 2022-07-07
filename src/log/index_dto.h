#pragma once

#include "log/common.h"

namespace faas {
namespace log {

struct IndexFoundResult {
    uint16_t view_id;
    uint16_t storage_shard_id;
    uint64_t seqnum;
};

struct IndexQuery {
    enum ReadDirection { kReadNext, kReadPrev, kReadNextB };
    ReadDirection direction;
    uint16_t origin_node_id;
    uint16_t hop_times;
    bool     initial;
    uint64_t client_data;

    uint32_t user_logspace;
    uint64_t user_tag;
    uint64_t query_seqnum;
    uint64_t metalog_progress;

    uint16_t master_node_id;
    uint16_t aggregate_type;

    IndexFoundResult prev_found_result;

    static ReadDirection DirectionFromOpType(protocol::SharedLogOpType op_type);
    protocol::SharedLogOpType DirectionToOpType() const;
    protocol::SharedLogOpType DirectionToIndexResult() const;
    std::string DirectionToString() const;
};

struct IndexQueryResult {
    enum State { kFound, kEmpty, kContinue, kMiss };
    State state;
    uint64_t metalog_progress;
    uint16_t next_view_id;

    IndexQuery       original_query;
    IndexFoundResult found_result;

    uint32_t StorageShardId() const;
    bool IsFound() const;
    bool IsPointHit() const;
};

using IndexQueryResultVec = absl::InlinedVector<IndexQueryResult, 4>;

}  // namespace log
}  // namespace faas