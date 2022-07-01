#include "log/index_dto.h"

namespace faas {
namespace log {

IndexQuery::ReadDirection IndexQuery::DirectionFromOpType(protocol::SharedLogOpType op_type) {
    switch (op_type) {
    case protocol::SharedLogOpType::READ_NEXT:
    case protocol::SharedLogOpType::READ_NEXT_INDEX_RESULT:
        return IndexQuery::kReadNext;
    case protocol::SharedLogOpType::READ_PREV:
    case protocol::SharedLogOpType::READ_PREV_INDEX_RESULT:
        return IndexQuery::kReadPrev;
    case protocol::SharedLogOpType::READ_NEXT_B:
    case protocol::SharedLogOpType::READ_NEXT_B_INDEX_RESULT:
        return IndexQuery::kReadNextB;
    case protocol::SharedLogOpType::READ_MIN:
        return IndexQuery::kReadNext;
    default:
        UNREACHABLE();
    }
}

protocol::SharedLogOpType IndexQuery::DirectionToOpType() const {
    switch (direction) {
    case IndexQuery::kReadNext:
        return protocol::SharedLogOpType::READ_NEXT;
    case IndexQuery::kReadPrev:
        return protocol::SharedLogOpType::READ_PREV;
    case IndexQuery::kReadNextB:
        return protocol::SharedLogOpType::READ_NEXT_B;
    default:
        UNREACHABLE();
    }
}

protocol::SharedLogOpType IndexQuery::DirectionToIndexResult() const {
    switch (direction) {
    case IndexQuery::kReadNext:
        return protocol::SharedLogOpType::READ_NEXT_INDEX_RESULT;
    case IndexQuery::kReadPrev:
        return protocol::SharedLogOpType::READ_PREV_INDEX_RESULT;
    case IndexQuery::kReadNextB:
        return protocol::SharedLogOpType::READ_NEXT_B_INDEX_RESULT;
    default:
        UNREACHABLE();
    }
}

std::string IndexQuery::DirectionToString() const {
    switch (direction) {
    case IndexQuery::kReadNext:
        return "next";
    case IndexQuery::kReadPrev:
        return "prev";
    case IndexQuery::kReadNextB:
        return "nextB";
    default:
        UNREACHABLE();
    }
}

uint32_t IndexQueryResult::StorageShardId() const {
    DCHECK_EQ(state, State::kFound);
    return bits::JoinTwo16(bits::LowHalf32(bits::HighHalf64(found_result.seqnum)), found_result.storage_shard_id);
}

bool IndexQueryResult::IsFound() const {
    return state == State::kFound;
}

bool IndexQueryResult::IsPointHit() const {
    if (state != State::kFound){
        return false;
    }
    return original_query.query_seqnum == found_result.seqnum;
}

}  // namespace log
}  // namespace faas