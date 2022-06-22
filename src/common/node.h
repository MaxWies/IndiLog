#pragma once

#include "base/common.h"
#include "common/time.h"
#include "utils/bits.h"

namespace faas {
namespace node {

enum NodeType {
    kGatewayNode     = 0,
    kEngineNode      = 1,
    kSequencerNode   = 2,
    kStorageNode     = 3,
    kIndexNode       = 4,
    kAggregatorNode  = 5,
    kTotalNodeType   = 6
};

static constexpr const char* kNodeTypeStr[] = {
    "GatewayNode",
    "EngineNode",
    "SequencerNode",
    "StorageNode",
    "IndexNode",
    "AggregatorNode"
};


}  // namespace node
}  // namespace faas