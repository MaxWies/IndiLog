#include "server/scale_watcher.h"

namespace faas {
namespace server {

using node::NodeType;

ScaleWatcher::ScaleWatcher() {}

ScaleWatcher::~ScaleWatcher() {}

void ScaleWatcher::StartWatching(zk::ZKSession* session) {
    watcher_.emplace(session, "scale");
    watcher_->SetNodeCreatedCallback(
        absl::bind_front(&ScaleWatcher::OnZNodeCreated, this));
    watcher_->Start();
}

void ScaleWatcher::SetNodeScaledCallback(NodeEventCallback cb) {
    node_scaled_cb_ = cb;
}

bool ScaleWatcher::ParseScaleOpPath(std::string_view path,
                                ScaleOp* scale_op, 
                                NodeType* node_type,
                                uint16_t* node_id) {
    std::string_view scale_prefix;
    if (absl::StartsWith(path, "in_")) {
        scale_prefix = "in_";
        *scale_op = kScaleIn;
    } else if (absl::StartsWith(path, "out_")) {
        scale_prefix = "out_";
        *scale_op = kScaleOut;
    } else {
        LOG(ERROR) << "Unknown scale operation: " << path;
        return false;
    }
    std::string_view node_sub_path = absl::StripPrefix(path, scale_prefix);
    std::string_view node_sub_prefix;
    if (absl::StartsWith(node_sub_path, "engine_")) {
        node_sub_prefix = "engine_";
        *node_type = NodeType::kEngineNode;
    } else if (absl::StartsWith(node_sub_path, "sequencer_")) {
        node_sub_prefix = "sequencer_";
        *node_type = NodeType::kSequencerNode;
    } else if (absl::StartsWith(node_sub_path, "storage_")) {
        node_sub_prefix = "storage_";
        *node_type = NodeType::kStorageNode;
    } else if (absl::StartsWith(node_sub_path, "index_")) {
        node_sub_prefix = "index_";
        *node_type = NodeType::kIndexNode;
    } else {
        LOG(ERROR) << "Unknown type of node: " << path;
        return false;
    }
    std::string_view prefix;
    prefix = absl::StrCat(scale_prefix, node_sub_prefix);
    int parsed;
    if (!absl::SimpleAtoi(absl::StripPrefix(path, prefix), &parsed)) {
        LOG(ERROR) << "Failed to parse node_id: " << path;
        return false;
    }
    *node_id = gsl::narrow_cast<uint16_t>(parsed);
    return true;
}

void ScaleWatcher::OnZNodeCreated(std::string_view path, std::span<const char> contents) {
    ScaleOp scale_op;
    NodeType node_type;
    uint16_t node_id;
    CHECK(ParseScaleOpPath(path, &scale_op, &node_type, &node_id));
    if (node_scaled_cb_) {
        node_scaled_cb_(scale_op, node_type, node_id);
    }
}

}  // namespace server
}  // namespace faas
