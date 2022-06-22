#include "log/storage_base.h"

#include "log/flags.h"
#include "server/constants.h"
#include "utils/fs.h"

#define log_header_ "StorageBase: "

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

StorageBase::StorageBase(uint16_t node_id)
    : ServerBase(node_id, fmt::format("storage_{}", node_id), NodeType::kStorageNode),
      node_id_(node_id),
      db_(nullptr),
      index_tier_only_(absl::GetFlag(FLAGS_slog_storage_index_tier_only)),
      per_tag_seqnum_min_completion_(absl::GetFlag(FLAGS_slog_activate_min_seqnum_completion)),
      background_thread_("BG", [this] { this->BackgroundThreadMain(); }) {}

StorageBase::~StorageBase() {}

void StorageBase::StartInternal() {
    SetupDB();
    SetupZKWatchers();
    SetupTimers();
    log_cache_.emplace(absl::GetFlag(FLAGS_slog_storage_cache_cap_mb));
    background_thread_.Start();
}

void StorageBase::StopInternal() {
    background_thread_.Join();
}

void StorageBase::SetupDB() {
    std::string db_backend = absl::GetFlag(FLAGS_slog_storage_backend);
    if (db_backend == "rocksdb") {
        db_.reset(new RocksDBBackend(db_path_));
    } else if (db_backend == "tkrzw_hash") {
        db_.reset(new TkrzwDBMBackend(TkrzwDBMBackend::kHashDBM, db_path_));
    } else if (db_backend == "tkrzw_tree") {
        db_.reset(new TkrzwDBMBackend(TkrzwDBMBackend::kTreeDBM, db_path_));
    } else if (db_backend == "tkrzw_skip") {
        db_.reset(new TkrzwDBMBackend(TkrzwDBMBackend::kSkipDBM, db_path_));
    } else {
        HLOG(FATAL) << "Unknown storage backend: " << db_backend;
    }
}

void StorageBase::SetupZKWatchers() {
    view_watcher_.SetViewCreatedCallback(
        [this] (const View* view) {
            this->OnViewCreated(view);
            // TODO: This is not always safe, try fix it
            for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                if (view->is_active_phylog(sequencer_id)) {
                    db_->InstallLogSpace(bits::JoinTwo16(view->id(), sequencer_id));
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

void StorageBase::SetupTimers() {
    CreatePeriodicTimer(
        kSendShardProgressTimerId,
        absl::Microseconds(absl::GetFlag(FLAGS_slog_local_cut_interval_us)),
        [this] () { this->SendShardProgressIfNeeded(); }
    );
}

void StorageBase::MessageHandler(const SharedLogMessage& message,
                                 std::span<const char> payload) {
    switch (SharedLogMessageHelper::GetOpType(message)) {
    case SharedLogOpType::READ_AT:
        HandleReadAtRequest(message);
        break;
    case SharedLogOpType::REPLICATE:
        HandleReplicateRequest(message, payload);
        break;
    case SharedLogOpType::METALOGS:
        OnRecvNewMetaLogs(message, payload);
        break;
    case SharedLogOpType::SET_AUXDATA:
        OnRecvLogAuxData(message, payload);
        break;
    case SharedLogOpType::REGISTER:
        OnRecvRegistration(message);
        break;
    default:
        UNREACHABLE();
    }
}

namespace {
static inline std::string SerializedLogEntry(const LogEntry& log_entry) {
    LogEntryProto log_entry_proto;
    log_entry_proto.set_user_logspace(log_entry.metadata.user_logspace);
    log_entry_proto.set_seqnum(log_entry.metadata.seqnum);
    log_entry_proto.set_localid(log_entry.metadata.localid);
    log_entry_proto.mutable_user_tags()->Add(
        log_entry.user_tags.begin(), log_entry.user_tags.end());
    log_entry_proto.set_data(log_entry.data);
    std::string data;
    CHECK(log_entry_proto.SerializeToString(&data));
    return data;
}
}  // namespace

std::optional<LogEntryProto> StorageBase::GetLogEntryFromDB(uint64_t seqnum) {
    auto data = db_->Get(bits::HighHalf64(seqnum), bits::LowHalf64(seqnum));
    if (!data.has_value()) {
        return std::nullopt;
    }
    LogEntryProto log_entry_proto;
    if (!log_entry_proto.ParseFromString(*data)) {
        HLOG(FATAL) << "Failed to parse LogEntryProto";
    }
    return log_entry_proto;
}

void StorageBase::PutLogEntryToDB(const LogEntry& log_entry) {
    uint64_t seqnum = log_entry.metadata.seqnum;
    std::string data = SerializedLogEntry(log_entry);
    db_->Put(bits::HighHalf64(seqnum), bits::LowHalf64(seqnum), STRING_AS_SPAN(data));
}

void StorageBase::LogCachePutAuxData(uint64_t seqnum, std::span<const char> data) {
    if (log_cache_.has_value()) {
        log_cache_->PutAuxData(seqnum, data);
    }
}

std::optional<std::string> StorageBase::LogCacheGetAuxData(uint64_t seqnum) {
    return log_cache_.has_value() ? log_cache_->GetAuxData(seqnum) : std::nullopt;
}

void StorageBase::SendIndexData(const View* view, const ViewMutable* view_mutable,
                                const IndexDataPackagesProto& index_data_packages) {
    uint32_t logspace_id = index_data_packages.logspace_id();
    DCHECK_EQ(view->id(), bits::HighHalf32(logspace_id));
    std::string serialized_data;
    CHECK(index_data_packages.SerializeToString(&serialized_data));
    SharedLogMessage message = SharedLogMessageHelper::NewIndexDataMessage(
        logspace_id);
    message.origin_node_id = node_id_;
    message.payload_size = gsl::narrow_cast<uint32_t>(serialized_data.size());
    if (!index_tier_only_){
        HVLOG(1) << "MetalogUpdate: Send index data to storage nodes";
        for (auto& [shard_id, engine_id] : view_mutable->GetStorageShardOccupation()) {
            if(bits::HighHalf32(shard_id) == bits::LowHalf32(logspace_id)){
                SendSharedLogMessage(protocol::ConnType::STORAGE_TO_ENGINE,
                                engine_id, message, STRING_AS_SPAN(serialized_data));
            }
        }
    }
    for(uint16_t index_id : view->GetIndexNodes()){
        bool send = per_tag_seqnum_min_completion_;
        if (!send) {
            uint16_t shard_id = index_id % view->num_index_shards();
            for(int i = 0; i < index_data_packages.index_data_proto_size(); i++){
                send |= ((index_data_packages.index_data_proto().at(i).metalog_position() - 1) % view->num_index_shards() == shard_id);
            }
        }
        if (send){
            HVLOG_F(1, "MetalogUpdate: Send index data to index node {}", index_id);
            SendSharedLogMessage(protocol::ConnType::STORAGE_TO_INDEX,
                index_id, message, STRING_AS_SPAN(serialized_data));
        }
    }
}

bool StorageBase::SendSequencerMessage(uint16_t sequencer_id,
                                       SharedLogMessage* message,
                                       std::span<const char> payload) {
    message->origin_node_id = node_id_;
    message->payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_SEQUENCER,
                                sequencer_id, *message, payload);
}

bool StorageBase::SendEngineResponse(const SharedLogMessage& request,
                                     SharedLogMessage* response,
                                     std::span<const char> payload1,
                                     std::span<const char> payload2,
                                     std::span<const char> payload3) {
    response->origin_node_id = node_id_;
    response->hop_times = request.hop_times + 1;
    response->payload_size = gsl::narrow_cast<uint32_t>(
        payload1.size() + payload2.size() + payload3.size());
    response->client_data = request.client_data;
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_ENGINE,
                                request.origin_node_id, *response,
                                payload1, payload2, payload3);
}

void StorageBase::SendRegistrationResponse(const SharedLogMessage& request, SharedLogMessage* response) {
    response->origin_node_id = node_id_;
    response->hop_times = request.hop_times + 1;
    response->payload_size = 0;
    SendSharedLogMessage(protocol::ConnType::STORAGE_TO_ENGINE, request.origin_node_id, *response);
}

void StorageBase::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                         const SharedLogMessage& message,
                                         std::span<const char> payload) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    DCHECK(
        (conn_type == kSequencerIngressTypeId && op_type == SharedLogOpType::METALOGS)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_AT)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::REPLICATE)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::SET_AUXDATA)
     || (conn_type == kIndexIngressTypeId && op_type == SharedLogOpType::READ_AT)
     || (conn_type == kAggregatorIngressTypeId && op_type == SharedLogOpType::READ_AT)
    ) << fmt::format("Invalid combination: conn_type={:#x}, op_type={:#x}",
                     conn_type, message.op_type);
    MessageHandler(message, payload);
}

bool StorageBase::SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                                       const SharedLogMessage& message,
                                       std::span<const char> payload1,
                                       std::span<const char> payload2,
                                       std::span<const char> payload3) {
    DCHECK_EQ(size_t{message.payload_size}, payload1.size() + payload2.size() + payload3.size());
    EgressHub* hub = CurrentIOWorkerChecked()->PickOrCreateConnection<EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        absl::bind_front(&StorageBase::CreateEgressHub, this, conn_type, dst_node_id));
    if (hub == nullptr) {
        HLOG_F(WARNING, "Failed to send shared log message. Hub is null. Connection type {}", gsl::narrow_cast<uint16_t>(conn_type));
        return false;
    }
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(SharedLogMessage));
    hub->SendMessage(data, payload1, payload2, payload3);
    return true;
}

void StorageBase::OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                                      int sockfd) {
    protocol::ConnType type = static_cast<protocol::ConnType>(handshake.conn_type);
    uint16_t src_node_id = handshake.src_node_id;

    switch (type) {
    case protocol::ConnType::ENGINE_TO_STORAGE:
        break;
    case protocol::ConnType::SEQUENCER_TO_STORAGE:
        break;
    case protocol::ConnType::INDEX_TO_STORAGE:
        break;
    case protocol::ConnType::AGGREGATOR_TO_STORAGE:
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
            absl::bind_front(&StorageBase::OnRecvSharedLogMessage, this,
                             conn_type_id & kConnectionTypeMask, src_node_id)));
    RegisterConnection(PickIOWorkerForConnType(conn_type_id), connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!ingress_conns_.contains(connection->id()));
    ingress_conns_[connection->id()] = std::move(connection);
}

void StorageBase::OnConnectionClose(ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    switch (connection->type() & kConnectionTypeMask) {
    case kSequencerIngressTypeId:
    case kEngineIngressTypeId:
    case kIndexIngressTypeId:
    case kAggregatorIngressTypeId:
        DCHECK(ingress_conns_.contains(connection->id()));
        ingress_conns_.erase(connection->id());
        break;
    case kSequencerEgressHubTypeId:
    case kEngineEgressHubTypeId:
    case kIndexEgressHubTypeId:
    case kAggregatorEgressHubTypeId:
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

EgressHub* StorageBase::CreateEgressHub(protocol::ConnType conn_type,
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

void StorageBase::OnNodeOffline(NodeType node_type, uint16_t node_id){
    if (node_type != NodeType::kEngineNode) {
        return;
    }
    int egress_hub_id = GetEgressHubTypeId(protocol::ConnType::STORAGE_TO_ENGINE, node_id);
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
