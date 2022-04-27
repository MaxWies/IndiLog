#pragma once

#include "base/common.h"
#include "common/node.h"
#include "common/protocol.h"
#include "common/zk.h"
#include "common/zk_utils.h"
#include "utils/socket.h"

namespace faas {
namespace server {

class ScaleWatcher final {
public:
    ScaleWatcher();
    ~ScaleWatcher();

    void StartWatching(zk::ZKSession* session);

    enum ScaleOp {
        kScaleOut = 0,
        kScaleIn = 1
    };

    using NodeEventCallback = std::function<void(ScaleOp, node::NodeType, uint16_t node_id)>;
    void SetNodeScaledCallback(NodeEventCallback cb);

private:
    std::optional<zk_utils::DirWatcher> watcher_;

    NodeEventCallback node_scaled_cb_;
    absl::Mutex mu_;

    bool ParseScaleOpPath(std::string_view path, ScaleOp* scale_op, node::NodeType* node_type, uint16_t* node_id);

    void OnZNodeCreated(std::string_view path, std::span<const char> contents);

    DISALLOW_COPY_AND_ASSIGN(ScaleWatcher);
};

}  // namespace server
}  // namespace faas
