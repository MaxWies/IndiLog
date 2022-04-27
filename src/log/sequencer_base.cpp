#include "log/sequencer_base.h"

#include "log/flags.h"
#include "server/constants.h"
#include "utils/bits.h"

#define log_header_ "SequencerBase: "

namespace faas {
namespace log {

using node::NodeType;

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

using server::IOWorker;
using server::ConnectionBase;
using server::IngressConnection;
using server::EgressHub;
using server::NodeWatcher;

SequencerBase::SequencerBase(uint16_t node_id)
    : ServerBase(node_id, fmt::format("sequencer_{}", node_id), node::kSequencerNode),
      node_id_(node_id) {}

SequencerBase::~SequencerBase() {}

void SequencerBase::StartInternal() {
    SetupZKWatchers();
    SetupTimers();
}

void SequencerBase::StopInternal() {}

void SequencerBase::SetupZKWatchers() {
    view_watcher_.SetViewCreatedCallback(
        [this] (const View* view) {
            this->OnViewCreated(view);
        }
    );
    view_watcher_.SetViewFrozenCallback(
        [this] (const View* view) {
            this->OnViewFrozen(view);
        }
    );
    view_watcher_.SetViewFinalizedCallback(
        [this] (const FinalizedView* finalized_view) {
            this->OnViewFinalized(finalized_view);
        }
    );
    view_watcher_.StartWatching(zk_session());
}

void SequencerBase::SetupTimers() {
    CreatePeriodicTimer(
        kMetaLogCutTimerId,
        absl::Microseconds(absl::GetFlag(FLAGS_slog_global_cut_interval_us)),
        [this] () { this->MarkNextCutIfDoable(); }
    );
}

void SequencerBase::MessageHandler(const SharedLogMessage& message,
                                   std::span<const char> payload) {
    switch (SharedLogMessageHelper::GetOpType(message)) {
    case SharedLogOpType::TRIM:
        HandleTrimRequest(message);
        break;
    case SharedLogOpType::META_PROG:
        OnRecvMetaLogProgress(message);
        break;
    case SharedLogOpType::SHARD_PROG:
        OnRecvShardProgress(message, payload);
        break;
    case SharedLogOpType::METALOGS:
        OnRecvNewMetaLogs(message, payload);
        break;
    case SharedLogOpType::REGISTER:
        OnRecvRegistration(message);
        break;
    default:
        UNREACHABLE();
    }
}

namespace {
static std::string SerializedMetaLogs(const MetaLogProto& metalog) {
    MetaLogsProto metalogs_proto;
    metalogs_proto.set_logspace_id(metalog.logspace_id());
    metalogs_proto.add_metalogs()->CopyFrom(metalog);
    std::string serialized;
    CHECK(metalogs_proto.SerializeToString(&serialized));
    return serialized;
}
}  // namespace

void SequencerBase::ReplicateMetaLog(const View* view, const MetaLogProto& metalog) {
    uint32_t logspace_id = metalog.logspace_id();
    DCHECK_EQ(bits::LowHalf32(logspace_id), my_node_id());
    SharedLogMessage message = SharedLogMessageHelper::NewMetaLogsMessage(logspace_id);
    std::string payload = SerializedMetaLogs(metalog);
    message.origin_node_id = node_id_;
    message.payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    const View::Sequencer* sequencer_node = view->GetSequencerNode(my_node_id());
    for (uint16_t sequencer_id : sequencer_node->GetReplicaSequencerNodes()) {
        bool success = SendSharedLogMessage(
            protocol::ConnType::SEQUENCER_TO_SEQUENCER, sequencer_id,
            message, STRING_AS_SPAN(payload));
        if (!success) {
            HLOG_F(ERROR, "Failed to send metalog message to sequencer {}", sequencer_id);
        }
    }
}

void SequencerBase::PropagateMetaLog(const View* view, const ViewMutable* view_mutable, const MetaLogProto& metalog) {
    uint32_t logspace_id = metalog.logspace_id();
    DCHECK_EQ(bits::LowHalf32(logspace_id), my_node_id());
    absl::flat_hash_set<uint16_t> engine_nodes;
    absl::flat_hash_set<uint16_t> storage_nodes;
    switch (metalog.type()) {
    case MetaLogProto::NEW_LOGS:
        for (const auto& [storage_shard_id, engine_node_id] : view_mutable->GetStorageShardOccupation()){
            engine_nodes.insert(engine_node_id);
            const View::StorageShard* storage_shard = view->GetStorageShard(storage_shard_id);
            for (uint16_t storage_id : storage_shard->GetStorageNodes()) {
                    storage_nodes.insert(storage_id);
            }
        }
        break;
    case MetaLogProto::TRIM:
        NOT_IMPLEMENTED();
        break;
    default:
        UNREACHABLE();
    }
    HVLOG_F(1, "Propagate metalog to {} engines and {} storage nodes", engine_nodes.size(), storage_nodes.size());
    SharedLogMessage message = SharedLogMessageHelper::NewMetaLogsMessage(metalog.logspace_id());
    std::string payload = SerializedMetaLogs(metalog);
    message.origin_node_id = node_id_;
    message.payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    for (uint16_t engine_id : engine_nodes) {
        bool success = SendSharedLogMessage(
            protocol::ConnType::SEQUENCER_TO_ENGINE, engine_id,
            message, STRING_AS_SPAN(payload));
        if (!success) {
            HLOG_F(ERROR, "Failed to send metalog message to engine {}", engine_id);
        }
    }
    for (uint16_t storage_id : storage_nodes) {
        bool success = SendSharedLogMessage(
            protocol::ConnType::SEQUENCER_TO_STORAGE, storage_id,
            message, STRING_AS_SPAN(payload));
        if (!success) {
            HLOG_F(ERROR, "Failed to send metalog message to storage {}", storage_id);
        }
    }
}

bool SequencerBase::SendSequencerMessage(uint16_t sequencer_id,
                                         SharedLogMessage* message,
                                         std::span<const char> payload) {
    message->origin_node_id = node_id_;
    message->payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    return SendSharedLogMessage(protocol::ConnType::SEQUENCER_TO_SEQUENCER,
                                sequencer_id, *message, payload);
}

bool SequencerBase::SendEngineResponse(const SharedLogMessage& request,
                                       SharedLogMessage* response,
                                       std::span<const char> payload) {
    response->origin_node_id = node_id_;
    response->hop_times = request.hop_times + 1;
    response->payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    response->client_data = request.client_data;
    return SendSharedLogMessage(protocol::ConnType::SEQUENCER_TO_ENGINE,
                                request.origin_node_id, *response, payload);
}

bool SequencerBase::SendRegistrationResponse(const SharedLogMessage& request, protocol::ConnType connection_type,
                                       SharedLogMessage* response) {
    response->origin_node_id = node_id_;
    response->hop_times = request.hop_times + 1;
    response->payload_size = 0;
    return SendSharedLogMessage(connection_type, request.origin_node_id, *response);
}

void SequencerBase::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                           const SharedLogMessage& message,
                                           std::span<const char> payload) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    DCHECK(
        (conn_type == kSequencerIngressTypeId && op_type == SharedLogOpType::METALOGS)
     || (conn_type == kSequencerIngressTypeId && op_type == SharedLogOpType::META_PROG)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::TRIM)
     || (conn_type == kStorageIngressTypeId && op_type == SharedLogOpType::SHARD_PROG)
    ) << fmt::format("Invalid combination: conn_type={:#x}, op_type={:#x}",
                     conn_type, message.op_type);
    MessageHandler(message, payload);
}

bool SequencerBase::SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                                         const SharedLogMessage& message,
                                         std::span<const char> payload) {
    DCHECK_EQ(size_t{message.payload_size}, payload.size());
    EgressHub* hub = CurrentIOWorkerChecked()->PickOrCreateConnection<EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        absl::bind_front(&SequencerBase::CreateEgressHub, this, conn_type, dst_node_id));
    if (hub == nullptr) {
        return false;
    }
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(SharedLogMessage));
    hub->SendMessage(data, payload);
    return true;
}

void SequencerBase::OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                                        int sockfd) {
    protocol::ConnType type = static_cast<protocol::ConnType>(handshake.conn_type);
    uint16_t src_node_id = handshake.src_node_id;

    switch (type) {
    case protocol::ConnType::ENGINE_TO_SEQUENCER:
        break;
    case protocol::ConnType::SEQUENCER_TO_SEQUENCER:
        break;
    case protocol::ConnType::STORAGE_TO_SEQUENCER:
        break;
    default:
        HLOG(ERROR) << "Invalid connection type: " << handshake.conn_type;
        close(sockfd);
        return;
    }

    int conn_type_id = ServerBase::GetIngressConnTypeId(type, src_node_id);
    auto connection = std::make_unique<IngressConnection>(
        conn_type_id, sockfd, sizeof(SharedLogMessage));
    connection->SetMessageFullSizeCallback(
        &IngressConnection::SharedLogMessageFullSizeCallback);
    connection->SetNewMessageCallback(
        IngressConnection::BuildNewSharedLogMessageCallback(
            absl::bind_front(&SequencerBase::OnRecvSharedLogMessage, this,
                             conn_type_id & kConnectionTypeMask, src_node_id)));
    RegisterConnection(PickIOWorkerForConnType(conn_type_id), connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!ingress_conns_.contains(connection->id()));
    ingress_conns_[connection->id()] = std::move(connection);
}

void SequencerBase::OnConnectionClose(ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    switch (connection->type() & kConnectionTypeMask) {
    case kSequencerIngressTypeId:
    case kEngineIngressTypeId:
    case kStorageIngressTypeId:
        DCHECK(ingress_conns_.contains(connection->id()));
        ingress_conns_.erase(connection->id());
        break;
    case kSequencerEgressHubTypeId:
    case kEngineEgressHubTypeId:
    case kStorageEgressHubTypeId:
        {
            absl::MutexLock lk(&conn_mu_);
            DCHECK(egress_hubs_.contains(connection->id()));
            egress_hubs_.erase(connection->id());
        }
        break;
    default:
        HLOG(FATAL) << "Unknown connection type: " << connection->type();
    }
}

EgressHub* SequencerBase::CreateEgressHub(protocol::ConnType conn_type,
                                          uint16_t dst_node_id,
                                          IOWorker* io_worker) {
    struct sockaddr_in addr;
    if (!node_watcher()->GetNodeAddr(NodeWatcher::GetDstNodeType(conn_type),
                                     dst_node_id, &addr)) {
        return nullptr;
    }
    auto egress_hub = std::make_unique<EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        &addr, absl::GetFlag(FLAGS_message_conn_per_worker));
    uint16_t src_node_id = node_id_;
    egress_hub->SetHandshakeMessageCallback(
        [conn_type, src_node_id] (std::string* handshake) {
            *handshake = protocol::EncodeHandshakeMessage(conn_type, src_node_id);
        }
    );
    RegisterConnection(io_worker, egress_hub.get());
    DCHECK_GE(egress_hub->id(), 0);
    EgressHub* hub = egress_hub.get();
    {
        absl::MutexLock lk(&conn_mu_);
        DCHECK(!egress_hubs_.contains(egress_hub->id()));
        egress_hubs_[egress_hub->id()] = std::move(egress_hub);
    }
    return hub;
}

void SequencerBase::OnNodeOffline(NodeType node_type, uint16_t node_id){
    if (node_type != NodeType::kEngineNode) {
        return;
    }
    int egress_hub_id = GetEgressHubTypeId(protocol::ConnType::SEQUENCER_TO_ENGINE, node_id);
    ForEachIOWorker([&] (IOWorker* io_worker) {
        EgressHub* egress_hub = io_worker->PickConnectionAs<EgressHub>(egress_hub_id);
        if(egress_hub != nullptr){
            HLOG(INFO) << "Close egress hub of offline node...";
            io_worker->ScheduleFunction(nullptr, [egress_hub = egress_hub]{
                egress_hub->ScheduleClose();
            });
            return;
        }
        HLOG(INFO) << "This IOWorker had no egress connection for this node";
    });
}

}  // namespace log
}  // namespace faas
