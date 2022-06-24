#include "log/indexing_base.h"

#include "log/flags.h"
#include "server/constants.h"
#include "utils/fs.h"

#define log_header_ "IndexBase: "

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

IndexBase::IndexBase(uint16_t node_id)
    : ServerBase(node_id, fmt::format("index_{}", node_id), NodeType::kIndexNode),
      node_id_(node_id) {}

IndexBase::~IndexBase() {}

void IndexBase::StartInternal() {
    SetupZKWatchers();
    SetupTimers();
}

void IndexBase::StopInternal() {}

void IndexBase::SetupZKWatchers() {
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

void IndexBase::SetupTimers() {
    
}

void IndexBase::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                        const SharedLogMessage& message,
                                        std::span<const char> payload) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    DCHECK(
        (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_NEXT)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_PREV)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_NEXT_B)
     || (conn_type == kStorageIngressTypeId && op_type == SharedLogOpType::INDEX_DATA)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_MIN)
     || (conn_type == kIndexIngressTypeId && op_type == SharedLogOpType::READ_NEXT_INDEX_RESULT)
     || (conn_type == kIndexIngressTypeId && op_type == SharedLogOpType::READ_PREV_INDEX_RESULT)
     || (conn_type == kIndexIngressTypeId && op_type == SharedLogOpType::READ_NEXT_B_INDEX_RESULT)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::REGISTER)
     || op_type == SharedLogOpType::RESPONSE
    ) << fmt::format("Invalid combination: conn_type={:#x}, op_type={:#x}",
                     conn_type, message.op_type);
    MessageHandler(message, payload);
}

void IndexBase::MessageHandler(const SharedLogMessage& message,
                                 std::span<const char> payload) {
    switch (SharedLogMessageHelper::GetOpType(message)) {
    case SharedLogOpType::READ_NEXT:
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
        HandleReadRequest(message);
        break;
    case SharedLogOpType::INDEX_DATA:
        OnRecvNewIndexData(message, payload);
        break;
    case SharedLogOpType::READ_NEXT_INDEX_RESULT:
    case SharedLogOpType::READ_PREV_INDEX_RESULT:
    case SharedLogOpType::READ_NEXT_B_INDEX_RESULT:
        HandleSlaveResult(message);
        break;
    case SharedLogOpType::READ_MIN:
        HandleReadMinRequest(message);
        break;
    case SharedLogOpType::REGISTER:
        OnRecvRegistration(message);
        break;
    default:
        LOG(ERROR) << "Operation type unknown";
        UNREACHABLE();
    }
}

void IndexBase::SendMasterIndexResult(const IndexQueryResult& result) {
    SharedLogMessage response = SharedLogMessageHelper::NewIndexResultResponse(result.original_query.DirectionToIndexResult());
    response.origin_node_id = my_node_id();
    response.hop_times = result.original_query.hop_times + 1;
    response.client_data = result.original_query.client_data;
    response.user_logspace = result.original_query.user_logspace;

    response.query_seqnum = result.original_query.query_seqnum;
    response.user_metalog_progress = result.metalog_progress;

    response.found_view_id = result.found_result.view_id;
    response.found_storage_shard_id = result.found_result.storage_shard_id;
    response.found_seqnum = result.found_result.seqnum;
    response.engine_node_id = result.original_query.origin_node_id;

    response.payload_size = 0;
    uint16_t master_node_id = result.original_query.master_node_id;

    protocol::ConnType connection_type = protocol::ConnType::INDEX_TO_AGGREGATOR;
    if (result.original_query.aggregate_type == protocol::kUseMasterSlave) {
        connection_type = protocol::ConnType::INDEX_TO_INDEX;
    }
    bool success = SendSharedLogMessage(
        connection_type,
        master_node_id, response);
    if (!success) {
        HLOG_F(WARNING, "IndexRead: Failed to send index result to master index {}", master_node_id);
    }
}

void IndexBase::SendIndexReadResponse(const IndexQueryResult& result, uint32_t logspace_id) {
    // protocol::SharedLogResultType result_type = protocol::SharedLogResultType::INDEX_OK;
    // SharedLogMessage response = SharedLogMessageHelper::NewResponse(result_type);
    // response.origin_node_id = my_node_id();
    // response.hop_times = result.original_query.hop_times + 1;
    // response.client_data = result.original_query.client_data;
    // response.logspace_id = logspace_id;
    // response.user_logspace = result.original_query.user_logspace;
    // response.query_tag = result.original_query.user_tag;
    // response.query_seqnum = result.original_query.query_seqnum;
    // response.prev_view_id = result.original_query.prev_found_result.view_id;
    // response.prev_shard_id = result.original_query.prev_found_result.storage_shard_id;
    // response.prev_found_seqnum = result.original_query.prev_found_result.seqnum;
    // std::string payload = SerializedIndexResult(result);
    // response.payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    // uint16_t engine_id = result.original_query.origin_node_id;
    // bool success = SendSharedLogMessage(
    //     protocol::ConnType::INDEX_TO_ENGINE,
    //     engine_id, response, payload);
    // if (!success) {
    //     HLOG_F(WARNING, "IndexRead: Failed to send index read response to engine {}", engine_id);
    // }
}

void IndexBase::SendIndexMinReadResponse(const SharedLogMessage& original_request, uint64_t seqnum, uint16_t storage_shard_id){
    SharedLogMessage response = SharedLogMessageHelper::NewResponse(protocol::SharedLogResultType::INDEX_MIN_OK);
    response.origin_node_id = my_node_id();

    response.hop_times = original_request.hop_times + 1;
    response.logspace_id = original_request.logspace_id;
    response.client_data = original_request.client_data;
    response.user_logspace = original_request.user_logspace;
    response.query_tag = original_request.query_tag;
    response.seqnum_timestamp = original_request.seqnum_timestamp; // timestamp

    response.min_seqnum = seqnum;
    response.found_storage_shard_id = storage_shard_id;

    uint16_t engine_destination = original_request.origin_node_id;
    bool success = SendSharedLogMessage(
        protocol::ConnType::INDEX_TO_ENGINE,
        engine_destination, response);
    if (!success) {
        HLOG_F(WARNING, "IndexRead: Failed to send index read response to engine {}", engine_destination);
    }
}

void IndexBase::BroadcastIndexReadResponse(const IndexQueryResult& result, const std::vector<uint16_t>& engine_ids, uint32_t logspace_id) {
    // protocol::SharedLogResultType result_type = protocol::SharedLogResultType::INDEX_OK;
    // SharedLogMessage response = SharedLogMessageHelper::NewResponse(result_type);
    // response.origin_node_id = my_node_id();
    // response.hop_times = result.original_query.hop_times + 1;
    // response.client_data = result.original_query.client_data;
    // response.logspace_id = logspace_id;
    // response.user_logspace = result.original_query.user_logspace;
    // response.query_tag = result.original_query.user_tag;
    // response.query_seqnum = result.original_query.query_seqnum;
    // response.prev_view_id = result.original_query.prev_found_result.view_id;
    // response.prev_shard_id = result.original_query.prev_found_result.storage_shard_id;
    // response.prev_found_seqnum = result.original_query.prev_found_result.seqnum;
    // std::string payload = SerializedIndexResult(result);
    // response.payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    // bool success = true;
    // for (uint16_t engine_id : engine_ids){
    //     success &= SendSharedLogMessage(
    //         protocol::ConnType::INDEX_TO_ENGINE,
    //         engine_id, response, payload
    //     );
    // }
    // if (!success) {
    //     HLOG(WARNING) << "IndexRead: Failed to send index read response to all engines";
    // }
}

void IndexBase::SendIndexReadFailureResponse(const IndexQuery& query, protocol::SharedLogResultType result) {
    SharedLogMessage response = SharedLogMessageHelper::NewResponse(result);
    response.origin_node_id = node_id_;
    response.hop_times = query.hop_times + 1;
    response.client_data = query.client_data;
    response.payload_size = 0; // necessary?
    uint16_t engine_id = query.origin_node_id;
    bool success = SendSharedLogMessage(
        protocol::ConnType::INDEX_TO_ENGINE,
        engine_id, response);
    if (!success) {
        HLOG_F(WARNING, "IndexRead: Failed to send index read failure response to engine={}", engine_id);
    }
    HVLOG_F(1, "IndexRead: Sent index read failure response to engine={}", engine_id);
}

bool IndexBase::SendStorageReadRequest(const IndexQueryResult& result,
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
            protocol::ConnType::INDEX_TO_STORAGE, storage_id, request);
        if (success) {
            return true;
        }
    }
    return false;
}

void IndexBase::SendRegistrationResponse(const SharedLogMessage& request, SharedLogMessage* response) {
    response->origin_node_id = node_id_;
    response->hop_times = request.hop_times + 1;
    response->payload_size = 0;
    SendSharedLogMessage(protocol::ConnType::INDEX_TO_ENGINE, request.origin_node_id, *response);
}

bool IndexBase::SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                                       const SharedLogMessage& message,
                                       std::span<const char> payload1) {
    DCHECK_EQ(size_t{message.payload_size}, payload1.size());
    EgressHub* hub = CurrentIOWorkerChecked()->PickOrCreateConnection<EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        absl::bind_front(&IndexBase::CreateEgressHub, this, conn_type, dst_node_id));
    if (hub == nullptr) {
        HLOG_F(WARNING, "Failed to send shared log message. Hub is null. Connection type {}", gsl::narrow_cast<uint16_t>(conn_type));
        return false;
    }
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(SharedLogMessage));
    hub->SendMessage(data, payload1);
    return true;
}

void IndexBase::OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                                      int sockfd) {
    protocol::ConnType type = static_cast<protocol::ConnType>(handshake.conn_type);
    uint16_t src_node_id = handshake.src_node_id;

    switch (type) {
    case protocol::ConnType::ENGINE_TO_INDEX:
        HVLOG(1) << "ConnectionType: EngineToIndex";
        break;
    case protocol::ConnType::STORAGE_TO_INDEX:
        HVLOG(1) << "ConnectionType: StorageToIndex";
        break;
    case protocol::ConnType::INDEX_TO_AGGREGATOR:
        HVLOG(1) << "ConnectionType: IndexToAggregator";
        break;
    case protocol::ConnType::INDEX_TO_INDEX:
        HVLOG(1) << "ConnectionType: IndexToIndex";
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
            absl::bind_front(&IndexBase::OnRecvSharedLogMessage, this,
                             conn_type_id & kConnectionTypeMask, src_node_id)));
    RegisterConnection(PickIOWorkerForConnType(conn_type_id), connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!ingress_conns_.contains(connection->id()));
    ingress_conns_[connection->id()] = std::move(connection);
}

void IndexBase::OnConnectionClose(ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    switch (connection->type() & kConnectionTypeMask) {
    case kEngineIngressTypeId:
    case kStorageIngressTypeId:
    case kAggregatorIngressTypeId:
    case kIndexIngressTypeId:
        DCHECK(ingress_conns_.contains(connection->id()));
        ingress_conns_.erase(connection->id());
        break;
    case kEngineEgressHubTypeId:
    case kStorageEgressHubTypeId:
    case kAggregatorEgressHubTypeId:
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

EgressHub* IndexBase::CreateEgressHub(protocol::ConnType conn_type,
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

void IndexBase::OnNodeOffline(NodeType node_type, uint16_t node_id){
    if (node_type != NodeType::kEngineNode) {
        return;
    }
    RemoveEngineNode(node_id);
    int egress_hub_id = GetEgressHubTypeId(protocol::ConnType::INDEX_TO_ENGINE, node_id);
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
