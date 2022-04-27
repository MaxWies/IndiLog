#include "base/init.h"
#include "base/common.h"
#include "common/flags.h"
#include "log/controller.h"

#include <signal.h>

ABSL_FLAG(uint32_t, random_seed, 23333, "Random seed");
ABSL_FLAG(size_t, metalog_replicas, 3, "Replicas for meta logs");
ABSL_FLAG(size_t, userlog_replicas, 3, "Replicas for users' logs");
ABSL_FLAG(size_t, index_replicas, 3, "Replicas for log index");
ABSL_FLAG(size_t, index_shards, 1, "Index shards");
ABSL_FLAG(size_t, num_phylogs, 1, "Number of physical logs");
ABSL_FLAG(size_t, max_num_storage_shards, 4, "Max number of storage shards of a physcial log");

namespace faas {

static std::atomic<log::Controller*> controller_ptr{nullptr};
static void StopControllerHandler() {
    log::Controller* controller = controller_ptr.exchange(nullptr);
    if (controller != nullptr) {
        controller->ScheduleStop();
    }
}

void ControllerMain(int argc, char* argv[]) {
    base::InitMain(argc, argv);
    base::SetInterruptHandler(StopControllerHandler);

    auto controller = std::make_unique<log::Controller>(
        absl::GetFlag(FLAGS_random_seed));

    controller->set_metalog_replicas(absl::GetFlag(FLAGS_metalog_replicas));
    controller->set_userlog_replicas(absl::GetFlag(FLAGS_userlog_replicas));
    controller->set_index_replicas(absl::GetFlag(FLAGS_index_replicas));
    controller->set_index_shards(absl::GetFlag(FLAGS_index_shards));
    controller->set_num_phylogs(absl::GetFlag(FLAGS_num_phylogs));
    controller->set_max_num_storage_shards(absl::GetFlag(FLAGS_max_num_storage_shards));

    controller->Start();
    controller_ptr.store(controller.get());
    controller->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::ControllerMain(argc, argv);
    return 0;
}
