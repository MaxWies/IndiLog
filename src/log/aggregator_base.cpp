#include "log/aggregator_base.h"

#include "log/flags.h"
#include "server/constants.h"
#include "utils/fs.h"

#define log_header_ "AggregatorBase: "

namespace faas {
namespace log {

using node::NodeType;

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

using server::IOWorker;
using server::ConnectionBase;
using server::IngressConnection;
using server::EgressHub;
using server::NodeWatcher;

AggregatorBase::AggregatorBase(uint16_t node_id)
    : ServerBase(node_id, fmt::format("aggregator_{}", node_id), NodeType::kAggregatorNode),
      node_id_(node_id) {}

AggregatorBase::~AggregatorBase() {}

void AggregatorBase::StartInternal() {
    SetupZKWatchers();
    SetupTimers();
}

void AggregatorBase::StopInternal() {}

void AggregatorBase::SetupZKWatchers() {
    view_watcher_.SetViewCreatedCallback(
        [this] (const View* view) {
            this->OnViewCreated(view);
            for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                if (view->is_active_phylog(sequencer_id)) {
                    // TODO
                }
            }
        }
    );
    view_watcher_.SetViewFinalizedCallback(
        [this] (const FinalizedView* finalized_view) {
            this->OnViewFinalized(finalized_view);
        }
    );
    view_watcher_.StartWatching(zk_session());
}

void AggregatorBase::SetupTimers() {
    
}

void AggregatorBase::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                        const SharedLogMessage& message,
                                        std::span<const char> payload) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    DCHECK(
        (conn_type == kIndexIngressTypeId && op_type == SharedLogOpType::READ_NEXT_INDEX_RESULT)
     || (conn_type == kIndexIngressTypeId && op_type == SharedLogOpType::READ_PREV_INDEX_RESULT)
     || (conn_type == kIndexIngressTypeId && op_type == SharedLogOpType::READ_NEXT_B_INDEX_RESULT)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::REGISTER)
     || op_type == SharedLogOpType::RESPONSE
    ) << fmt::format("Invalid combination: conn_type={:#x}, op_type={:#x}",
                     conn_type, message.op_type);
    MessageHandler(message, payload);
}

void AggregatorBase::MessageHandler(const SharedLogMessage& message,
                                 std::span<const char> payload) {
    switch (SharedLogMessageHelper::GetOpType(message)) {
    case SharedLogOpType::READ_NEXT_INDEX_RESULT:
    case SharedLogOpType::READ_PREV_INDEX_RESULT:
    case SharedLogOpType::READ_NEXT_B_INDEX_RESULT:
        HandleSlaveResult(message);
        break;
    case SharedLogOpType::REGISTER:
        OnRecvRegistration(message);
        break;
    default:
        LOG(ERROR) << "Operation type unknown";
        UNREACHABLE();
    }
}
void AggregatorBase::SendIndexReadFailureResponse(const IndexQuery& query, protocol::SharedLogResultType result) {
    SharedLogMessage response = SharedLogMessageHelper::NewResponse(result);
    response.origin_node_id = node_id_;
    response.hop_times = query.hop_times + 1;
    response.client_data = query.client_data;
    response.payload_size = 0; // necessary?
    uint16_t engine_id = query.origin_node_id;
    bool success = SendSharedLogMessage(
        protocol::ConnType::AGGREGATOR_TO_ENGINE,
        engine_id, response);
    if (!success) {
        HLOG_F(WARNING, "IndexRead: Failed to send index read failure response to engine={}", engine_id);
    }
    HVLOG_F(1, "IndexRead: Sent index read failure response to engine={}", engine_id);
}

bool AggregatorBase::SendStorageReadRequest(const IndexQueryResult& result,
                                        const View::StorageShard* storage_shard_node) {
    HVLOG_F(1, "IndexRead: Send StorageReadRequest for seqnum={}", bits::HexStr0x(result.found_result.seqnum));
    static constexpr int kMaxRetries = 3;
    DCHECK(result.state == IndexQueryResult::kFound);

    uint64_t seqnum = result.found_result.seqnum;
    SharedLogMessage request = SharedLogMessageHelper::NewReadAtMessage(
        bits::HighHalf64(seqnum), bits::LowHalf64(seqnum));
    request.user_metalog_progress = result.metalog_progress;
    request.storage_shard_id = storage_shard_node->local_shard_id();
    request.origin_node_id = result.original_query.origin_node_id;
    request.hop_times = result.original_query.hop_times + 1;
    request.client_data = result.original_query.client_data;
    for (int i = 0; i < kMaxRetries; i++) {
        uint16_t storage_id = storage_shard_node->PickStorageNode();
        HVLOG_F(1, "IndexRead: Forward read request on behalf of engine_node={} to storage_node={}", request.origin_node_id, storage_id);
        bool success = SendSharedLogMessage(
            protocol::ConnType::AGGREGATOR_TO_STORAGE, storage_id, request);
        if (success) {
            return true;
        }
    }
    return false;
}

void AggregatorBase::SendRegistrationResponse(const SharedLogMessage& request, SharedLogMessage* response) {
    response->origin_node_id = node_id_;
    response->hop_times = request.hop_times + 1;
    response->payload_size = 0;
    SendSharedLogMessage(protocol::ConnType::AGGREGATOR_TO_ENGINE, request.origin_node_id, *response);
}

bool AggregatorBase::SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                                       const SharedLogMessage& message,
                                       std::span<const char> payload1) {
    DCHECK_EQ(size_t{message.payload_size}, payload1.size());
    EgressHub* hub = CurrentIOWorkerChecked()->PickOrCreateConnection<EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        absl::bind_front(&AggregatorBase::CreateEgressHub, this, conn_type, dst_node_id));
    if (hub == nullptr) {
        HLOG_F(WARNING, "Failed to send shared log message. Hub is null. Connection type {}", gsl::narrow_cast<uint16_t>(conn_type));
        return false;
    }
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(SharedLogMessage));
    hub->SendMessage(data, payload1);
    return true;
}

void AggregatorBase::OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                                      int sockfd) {
    protocol::ConnType type = static_cast<protocol::ConnType>(handshake.conn_type);
    uint16_t src_node_id = handshake.src_node_id;

    switch (type) {
    case protocol::ConnType::INDEX_TO_AGGREGATOR:
        break;
    case protocol::ConnType::ENGINE_TO_AGGREGATOR:
        break;
    default:
        HLOG(ERROR) << "Invalid connection type: " << handshake.conn_type;
        close(sockfd);
        return;
    }

    int conn_type_id = AggregatorBase::GetIngressConnTypeId(type, src_node_id);
    auto connection = std::make_unique<IngressConnection>(
        conn_type_id, sockfd, sizeof(SharedLogMessage));
    connection->SetMessageFullSizeCallback(
        &IngressConnection::SharedLogMessageFullSizeCallback);
    connection->SetNewMessageCallback(
        IngressConnection::BuildNewSharedLogMessageCallback(
            absl::bind_front(&AggregatorBase::OnRecvSharedLogMessage, this,
                             conn_type_id & kConnectionTypeMask, src_node_id)));
    RegisterConnection(PickIOWorkerForConnType(conn_type_id), connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!ingress_conns_.contains(connection->id()));
    ingress_conns_[connection->id()] = std::move(connection);
}

void AggregatorBase::OnConnectionClose(ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    switch (connection->type() & kConnectionTypeMask) {
    case kEngineIngressTypeId:
    case kStorageIngressTypeId:
    case kIndexIngressTypeId:
        DCHECK(ingress_conns_.contains(connection->id()));
        ingress_conns_.erase(connection->id());
        break;
    case kEngineEgressHubTypeId:
    case kStorageEgressHubTypeId:
    case kIndexEgressHubTypeId:
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

EgressHub* AggregatorBase::CreateEgressHub(protocol::ConnType conn_type,
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

void AggregatorBase::OnNodeOffline(NodeType node_type, uint16_t node_id){
    if (node_type != NodeType::kEngineNode) {
        return;
    }
    RemoveEngineNode(node_id);
    int egress_hub_id = GetEgressHubTypeId(protocol::ConnType::AGGREGATOR_TO_ENGINE, node_id);
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
