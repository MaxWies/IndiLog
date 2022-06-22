#include "base/init.h"
#include "base/common.h"
#include "utils/env_variables.h"
#include "log/aggregator.h"

ABSL_FLAG(int, node_id, -1,
          "My node ID. Also settable through environment variable FAAS_NODE_ID.");

namespace faas {

static std::atomic<server::ServerBase*> server_ptr{nullptr};
static void StopServerHandler() {
    server::ServerBase* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

void AggregatorMain(int argc, char* argv[]) {
    base::InitMain(argc, argv);
    base::SetInterruptHandler(StopServerHandler);

    int node_id = absl::GetFlag(FLAGS_node_id);
    if (node_id == -1) {
        node_id = utils::GetEnvVariableAsInt("FAAS_NODE_ID", -1);
    }
    if (node_id == -1) {
        LOG(FATAL) << "Node ID not set!";
    }
    auto aggregator = std::make_unique<log::Aggregator>(node_id);

    aggregator->Start();
    server_ptr.store(aggregator.get());
    aggregator->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::AggregatorMain(argc, argv);
    return 0;
}
