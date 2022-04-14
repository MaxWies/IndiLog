#include "base/init.h"
#include "base/common.h"
#include "utils/env_variables.h"
#include "log/storage.h"
#include <filesystem>

ABSL_FLAG(int, node_id, -1,
          "My node ID. Also settable through environment variable FAAS_NODE_ID.");
ABSL_FLAG(std::string, db_path, "", "Path for RocksDB database storage.");

namespace faas {

static std::atomic<server::ServerBase*> server_ptr{nullptr};
static void StopServerHandler() {
    server::ServerBase* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

//TODO: remove later
void delete_dir_content(const std::filesystem::path& dir_path) {
    for (auto& path: std::filesystem::directory_iterator(dir_path)) {
        std::filesystem::remove_all(path);
    }
}

void StorageMain(int argc, char* argv[]) {
    base::InitMain(argc, argv);
    base::SetInterruptHandler(StopServerHandler);

    delete_dir_content(absl::GetFlag(FLAGS_db_path));

    int node_id = absl::GetFlag(FLAGS_node_id);
    if (node_id == -1) {
        node_id = utils::GetEnvVariableAsInt("FAAS_NODE_ID", -1);
    }
    if (node_id == -1) {
        LOG(FATAL) << "Node ID not set!";
    }
    auto storage = std::make_unique<log::Storage>(node_id);

    storage->set_db_path(absl::GetFlag(FLAGS_db_path));

    storage->Start();
    server_ptr.store(storage.get());
    storage->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::StorageMain(argc, argv);
    return 0;
}
