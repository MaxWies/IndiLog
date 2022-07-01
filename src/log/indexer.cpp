#include "log/indexer.h"

#include "log/flags.h"
#include "utils/bits.h"
#include "utils/io.h"
#include "utils/timerfd.h"

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

Indexer::Indexer(uint16_t node_id)
    : IndexerBase(node_id),
      log_header_(fmt::format("Indexer[{}-N]: ", node_id)),
      current_view_(nullptr),
      current_view_active_(false),
      per_tag_seqnum_min_completion_(absl::GetFlag(FLAGS_slog_activate_min_seqnum_completion)) {}

Indexer::~Indexer() {}

void Indexer::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    bool contains_myself = view->contains_index_node(my_node_id());
    if (!contains_myself) {
        HLOG_F(WARNING, "View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                if (!view->is_active_phylog(sequencer_id)) {
                    continue;
                }
                //TODO: currently all index nodes have indexes for active sequencers
                HLOG_F(INFO, "Create logspace for view {} and sequencer {}", view->id(), sequencer_id);
                index_collection_.InstallLogSpace(std::make_unique<IndexShard>(view, sequencer_id, my_node_id() % view->num_index_shards(), view->num_index_shards()));
                view_mutable_.InitializeCurrentEngineNodeIds(sequencer_id);
            }
        }
        future_requests_.OnNewView(view, contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        if (contains_myself) {
            current_view_active_ = true;
        }
        views_.push_back(view);
        log_header_ = fmt::format("Index[{}-{}]: ", my_node_id(), view->id());
    }
    if (!ready_requests.empty()) {
        HLOG_F(INFO, "{} requests for the new view", ready_requests.size());
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, requests = std::move(ready_requests)] () {
                ProcessRequests(requests);
            }
        );
    }
}

void Indexer::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    IndexQueryResultVec index_query_results;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        index_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &index_query_results] (uint32_t logspace_id,
                                              LockablePtr<IndexShard> index_ptr) {
                log_utils::FinalizedLogSpace<IndexShard>(
                    index_ptr, finalized_view);
                auto locked_index = index_ptr.Lock();
                locked_index->PollQueryResults(&index_query_results);
            }
        );
    }
    if (!index_query_results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, results = std::move(index_query_results)] {
                ProcessIndexQueryResults(results);
            }
        );
    }
}

#define ONHOLD_IF_FROM_FUTURE_VIEW(MESSAGE_VAR, PAYLOAD_VAR)        \
    do {                                                            \
        if (current_view_ == nullptr                                \
                || (MESSAGE_VAR).view_id > current_view_->id()) {   \
            future_requests_.OnHoldRequest(                         \
                (MESSAGE_VAR).view_id,                              \
                SharedLogRequest(MESSAGE_VAR, PAYLOAD_VAR));        \
            return;                                                 \
        }                                                           \
    } while (0)

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)                       \
    do {                                                            \
        if (current_view_ != nullptr                                \
                && (MESSAGE_VAR).view_id < current_view_->id()) {   \
            HLOG_F(WARNING, "Receive outdate request from view {}", \
                   (MESSAGE_VAR).view_id);                          \
            return;                                                 \
        }                                                           \
    } while (0)

#define RETURN_IF_LOGSPACE_FINALIZED(LOGSPACE_PTR)                  \
    do {                                                            \
        if ((LOGSPACE_PTR)->finalized()) {                          \
            uint32_t logspace_id = (LOGSPACE_PTR)->identifier();    \
            HLOG_F(WARNING, "LogSpace {} is finalized",             \
                   bits::HexStr0x(logspace_id));                    \
            return;                                                 \
        }                                                           \
    } while (0)

void Indexer::HandleReadRequest(const SharedLogMessage& request) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(request);
    DCHECK(  op_type == SharedLogOpType::READ_NEXT
          || op_type == SharedLogOpType::READ_PREV
          || op_type == SharedLogOpType::READ_NEXT_B);
    DCHECK(  request.aggregator_type == protocol::kUseMasterSlave 
          || request.aggregator_type == protocol::kUseAggregator);
    LockablePtr<IndexShard> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
        // TODO: index nodes have all physical log spaces?
        index_ptr = index_collection_.GetLogSpaceChecked(request.logspace_id);
    }
    if (index_ptr != nullptr) {
        IndexQuery query = BuildIndexQuery(request, request.origin_node_id);
        HVLOG_F(1, "IndexRead: Make query. client_id={}, metalog={}, logspace={}, tag={}, query_seqnum={}", 
            request.client_data, bits::HexStr0x(query.metalog_progress), request.logspace_id, query.user_tag, bits::HexStr0x(query.query_seqnum)
        );
        IndexQueryResultVec query_results;
        {
            auto locked_index = index_ptr.Lock();
            locked_index->MakeQuery(query);
            locked_index->PollQueryResults(&query_results);
        }
        ProcessIndexQueryResults(query_results);
    } else {
        HVLOG_F(1, "IndexRead: There is no local index for logspace {}", request.logspace_id);
        IndexQuery query = BuildIndexQuery(request, request.origin_node_id);
        SendIndexReadFailureResponse(query, protocol::SharedLogResultType::EMPTY);
    }
}

void Indexer::HandleReadMinRequest(const SharedLogMessage& request) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(request);
    DCHECK(op_type == SharedLogOpType::READ_MIN);
    if (!per_tag_seqnum_min_completion_) {
        LOG(FATAL) << "Per tag min seqnum completion is deactivated";
    }
    HVLOG_F(1, "IndexRead: Make query for min seqnum tag={}", request.query_tag);
    uint64_t found_seqnum = kInvalidLogSeqNum;
    uint16_t found_storage_shard_id = 0;
    PerTagMinSeqnumTable::const_accessor accessor;
    {
        if(per_tag_min_seqnum_table_.find(accessor, request.query_tag)){
            HVLOG_F(1, "Tag={} is not new", request.query_tag);
            found_seqnum = accessor->second.seqnum;
            found_storage_shard_id = accessor->second.storage_shard_id;
        }
    }
    SendIndexMinReadResponse(request, found_seqnum, found_storage_shard_id);
}

void Indexer::OnRecvNewIndexData(const SharedLogMessage& message,
                                     std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::INDEX_DATA);
    HVLOG_F(1, "IndexUpdate: Index data received from storage_node={}", message.origin_node_id);
    IndexDataPackagesProto index_data_packages;
    if (!index_data_packages.ParseFromArray(payload.data(),
                                         static_cast<int>(payload.size()))) {
        LOG(FATAL) << "IndexUpdate: Failed to parse IndexDataProto";
    }
    const View* view = nullptr;
    IndexQueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        DCHECK(message.view_id < views_.size());
        view = views_.at(message.view_id);
        uint16_t shard_id = my_node_id() % view->num_index_shards();
        std::vector<int> relevant_packages;
        for (int i = 0; i < index_data_packages.index_data_proto_size(); i++) {
            if ((index_data_packages.index_data_proto().at(i).metalog_position() - 1) % view->num_index_shards() == shard_id) {
                relevant_packages.push_back(i);
            }
        }
        if (!relevant_packages.empty()){
            const View::Storage* storage_node = view->GetStorageNode(message.origin_node_id);
            View::NodeIdVec storage_shards = storage_node->GetLocalStorageShardIds(message.sequencer_id);
            auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
            {
                auto locked_index = index_ptr.Lock();
                for (int i : relevant_packages) {
                    locked_index->AdvanceIndexProgress(index_data_packages.index_data_proto().at(i));
                }
                locked_index->PollQueryResults(&query_results);
            }
        }
    }
    ProcessIndexQueryResults(query_results);
    if (per_tag_seqnum_min_completion_) {
        for (const IndexDataProto& index_data : index_data_packages.index_data_proto()){
            FilterNewTags(view, index_data_packages.logspace_id(), index_data);
        }
    }
}

void Indexer::FilterNewTags(const View* view, uint32_t logspace_id, const IndexDataProto& index_data) {
    int n = index_data.seqnum_halves_size();
    auto tag_iter = index_data.user_tags().begin();
    for (int i = 0; i < n; i++) {
        size_t num_tags = index_data.user_tag_sizes(i);
        auto tags = UserTagVec(tag_iter, tag_iter + num_tags);
        std::vector<uint64_t> filtered_tags;
        for(uint64_t tag : tags){
            if (tag % view->num_index_shards() == my_node_id() % view->num_index_shards()){
                uint64_t seqnum = bits::JoinTwo32(logspace_id, index_data.seqnum_halves(i));
                uint16_t storage_shard_id = gsl::narrow_cast<uint16_t>(index_data.engine_ids(i));
                PerTagMinSeqnum entry = PerTagMinSeqnum({
                    seqnum,
                    storage_shard_id
                });
                PerTagMinSeqnumTable::accessor accessor;
                if (per_tag_min_seqnum_table_.insert(accessor, tag)){
                    accessor->second = entry;
                } else {
                    if(accessor->second.seqnum > seqnum){
                        accessor->second = entry;
                    }
                }
            }
        }
        tag_iter += num_tags;
    }
}

void Indexer::OnRecvRegistration(const protocol::SharedLogMessage& received_message) {
    DCHECK(SharedLogMessageHelper::GetOpType(received_message) == SharedLogOpType::REGISTER);
    DCHECK(SharedLogMessageHelper::GetResultType(received_message) == SharedLogResultType::REGISTER_ENGINE);
    absl::MutexLock view_lk(&view_mu_);
    if(received_message.view_id != current_view_->id()){
        HLOG_F(WARNING, "Current view not the same. register_view={}, my_current_view={}", received_message.view_id, current_view_->id());
        HLOG(WARNING) << "Registration failed";
        SharedLogMessage response = SharedLogMessageHelper::NewRegisterResponseMessage(
            SharedLogResultType::REGISTER_INDEX_FAILED,
            current_view_->id(),
            received_message.sequencer_id,
            received_message.storage_shard_id,
            received_message.engine_node_id,
            received_message.local_start_id
        );
        SendRegistrationResponse(received_message, &response);
        return;
    }
    DCHECK_EQ(received_message.origin_node_id, received_message.engine_node_id);
    if(!view_mutable_.PutCurrentEngineNodeId(received_message.sequencer_id, received_message.engine_node_id)){
        HLOG_F(WARNING, "Engine with id={} already registered", received_message.engine_node_id);
    }
    if(ongoing_engine_index_reads_.contains(received_message.engine_node_id)){
        HLOG_F(WARNING, "Remove index read operations of engine with id={}", received_message.engine_node_id);
        ongoing_engine_index_reads_.erase(received_message.engine_node_id);
    }
    EngineIndexReadOp* engine_index_read_ops = new EngineIndexReadOp();
    ongoing_engine_index_reads_[received_message.engine_node_id].reset(engine_index_read_ops);
    HLOG_F(INFO, "Registration ok. engine_id={}", received_message.engine_node_id);
    SharedLogMessage response = SharedLogMessageHelper::NewRegisterResponseMessage(
            SharedLogResultType::REGISTER_INDEX_OK,
            received_message.view_id,
            received_message.sequencer_id,
            received_message.storage_shard_id,
            received_message.engine_node_id,
            received_message.local_start_id
    );
    SendRegistrationResponse(received_message, &response);
}

void Indexer::RemoveEngineNode(uint16_t engine_node_id){
    absl::MutexLock view_lk(&view_mu_);
    view_mutable_.RemoveCurrentEngineNodeId(engine_node_id);
    if (ongoing_engine_index_reads_.contains(engine_node_id)){
        ongoing_engine_index_reads_.erase(engine_node_id);
    }
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW
#undef RETURN_IF_LOGSPACE_FINALIZED

void Indexer::ProcessIndexResult(const IndexQueryResult& my_query_result) {
    if (my_query_result.IsPointHit()){
        ForwardReadRequest(my_query_result);
    }
    if (my_query_result.original_query.aggregate_type == protocol::kUseAggregator || my_query_result.original_query.master_node_id != my_node_id()){
        SendMasterIndexResult(my_query_result);
        return;
    } 
    IndexQueryResult aggregated_index_query_result;
    bool aggregate_complete = AggregateIndexResult(my_node_id(), my_query_result, &aggregated_index_query_result);
    if (aggregate_complete && aggregated_index_query_result.IsFound() && !aggregated_index_query_result.IsPointHit()) {
        ForwardReadRequest(aggregated_index_query_result);
    } else if (aggregate_complete && !aggregated_index_query_result.IsFound()){
        SendIndexReadFailureResponse(aggregated_index_query_result.original_query, protocol::SharedLogResultType::EMPTY);
    }
}

bool Indexer::AggregateIndexResult(const uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* aggregated_index_query_result){
    HVLOG_F(1, "IndexRead: Index result received. index_node={}, engine_node={}, client_key={}, query_seqnum={}", 
        index_node_id_other, index_query_result_other.original_query.origin_node_id, bits::HexStr0x(index_query_result_other.original_query.client_data), 
        bits::HexStr0x(index_query_result_other.original_query.query_seqnum)
    );
    EngineIndexReadOp* engine_index_read_op = nullptr;
    size_t num_index_shards; 
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        num_index_shards = current_view_->num_index_shards();
        engine_index_read_op = ongoing_engine_index_reads_.at(index_query_result_other.original_query.origin_node_id).get();
    }
    if (engine_index_read_op == nullptr) {
        HLOG_F(ERROR, "No operations entry for engine_node={}", index_query_result_other.original_query.origin_node_id);
        return false;
    }
    return engine_index_read_op->Aggregate(num_index_shards, index_node_id_other, index_query_result_other, aggregated_index_query_result);
}

void Indexer::HandleSlaveResult(const protocol::SharedLogMessage& message){
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::READ_NEXT_INDEX_RESULT || 
           SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::READ_PREV_INDEX_RESULT ||
           SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::READ_NEXT_B_INDEX_RESULT);
    IndexQueryResult slave_index_query_result = BuildIndexResult(message);
    IndexQueryResult aggregated_index_query_result;
    bool aggregate_complete = AggregateIndexResult(message.origin_node_id, slave_index_query_result, &aggregated_index_query_result);
    if (aggregate_complete){
        if (aggregated_index_query_result.IsFound() && !aggregated_index_query_result.IsPointHit()){
            ForwardReadRequest(aggregated_index_query_result);
        } else if (!aggregated_index_query_result.IsFound()){
            SendIndexReadFailureResponse(aggregated_index_query_result.original_query, protocol::SharedLogResultType::EMPTY);
        }
    }
}

void Indexer::ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                           IndexQueryResultVec* more_results) {
    DCHECK(query_result.state == IndexQueryResult::kContinue);
    HVLOG_F(1, "IndexRead: Process IndexContinueResult: next_view_id={}",
            query_result.next_view_id);
    const IndexQuery& query = query_result.original_query;
    LockablePtr<IndexShard> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        uint16_t view_id = query_result.next_view_id;
        if (view_id >= views_.size()) {
            HLOG_F(FATAL, "Cannot find view {}", view_id);
        }
        const View* view = views_.at(view_id);
        uint32_t logspace_id = view->LogSpaceIdentifier(query.user_logspace);
        // TODO: index node has index for all sequencers?
        index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
    }
    if (index_ptr != nullptr) {
        IndexQuery query = BuildIndexQuery(query_result);
        auto locked_index = index_ptr.Lock();
        locked_index->MakeQuery(query);
        locked_index->PollQueryResults(more_results);
    } else {
        HLOG(ERROR) << "IndexRead: No index for logspace";
    }
}

void Indexer::ForwardReadRequest(const IndexQueryResult& query_result){
    DCHECK(query_result.state == IndexQueryResult::kFound);
    const View::StorageShard* storage_shard = nullptr;
    uint32_t logspace_id;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        uint16_t view_id = query_result.found_result.view_id;
        if (view_id < views_.size()) {
            const View* view = views_.at(view_id);
            storage_shard = view->GetStorageShard(query_result.StorageShardId());
            logspace_id = view->LogSpaceIdentifier(query_result.original_query.user_logspace);
        } else {
            HLOG_F(FATAL, "IndexRead: Cannot find view {}", view_id);
        }
    }
    bool success = SendStorageReadRequest(query_result, storage_shard);
    if (!success) {
        uint64_t seqnum = query_result.found_result.seqnum;
        IndexQuery query = query_result.original_query;
        HLOG_F(WARNING, "IndexRead: Failed to send read request for seqnum {} ", bits::HexStr0x(seqnum));
        SendIndexReadFailureResponse(query, protocol::SharedLogResultType::DATA_LOST);
    }
}

void Indexer::ProcessIndexQueryResults(const IndexQueryResultVec& results) {
    IndexQueryResultVec more_results;
    for (const IndexQueryResult& result : results) {
        switch (result.state) {
        case IndexQueryResult::kEmpty:
        case IndexQueryResult::kFound:
            ProcessIndexResult(result);
            break;
        case IndexQueryResult::kContinue:
            ProcessIndexContinueResult(result, &more_results);
            break;
        default:
            UNREACHABLE();
        }
    }
    if (!more_results.empty()) {
        ProcessIndexQueryResults(more_results);
    }
}

void Indexer::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_AS_SPAN(request.payload));
    }
}

// not used so far
SharedLogMessage Indexer::BuildReadRequestMessage(const IndexQueryResult& result) {
    DCHECK(result.state == IndexQueryResult::kContinue);
    IndexQuery query = result.original_query;
    SharedLogMessage request = SharedLogMessageHelper::NewReadMessage(
        query.DirectionToOpType());
    request.origin_node_id = query.origin_node_id;
    request.hop_times = query.hop_times + 1;
    request.client_data = query.client_data;
    request.user_logspace = query.user_logspace;
    request.query_tag = query.user_tag;
    request.query_seqnum = query.query_seqnum;
    request.user_metalog_progress = result.metalog_progress;
    request.prev_view_id = result.found_result.view_id;
    request.prev_shard_id = result.found_result.storage_shard_id;
    request.prev_found_seqnum = result.found_result.seqnum;
    return request;
}

IndexQuery Indexer::BuildIndexQuery(const SharedLogMessage& message, const uint16_t original_requester_id) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    IndexQuery index_query = IndexQuery {
        .direction = IndexQuery::DirectionFromOpType(op_type),
        .origin_node_id = original_requester_id,
        .hop_times = message.hop_times,
        .initial = (message.flags | protocol::kReadInitialFlag) != 0,
        .client_data = message.client_data,
        .user_logspace = message.user_logspace,
        .user_tag = message.query_tag,
        .query_seqnum = message.query_seqnum,
        .metalog_progress = message.user_metalog_progress,
        .master_node_id = message.aggregator_node_id,
        .prev_found_result = IndexFoundResult {
            .view_id = message.prev_view_id,
            .storage_shard_id = message.prev_shard_id,
            .seqnum = message.prev_found_seqnum
        },
    };
    if(message.aggregator_type == protocol::kUseAggregator || message.aggregator_type == protocol::kUseMasterSlave){
        index_query.master_node_id = message.aggregator_node_id;
        index_query.aggregate_type = message.aggregator_type;
        index_query.prev_found_result = IndexFoundResult {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = message.prev_found_seqnum
        };
    } else {
        index_query.prev_found_result = IndexFoundResult {
            .view_id = message.prev_view_id,
            .storage_shard_id = message.prev_shard_id,
            .seqnum = message.prev_found_seqnum
        };
    }
    return index_query;
}

IndexQuery Indexer::BuildIndexQuery(const IndexQueryResult& result) {
    DCHECK(result.state == IndexQueryResult::kContinue);
    IndexQuery query = result.original_query;
    query.initial = false;
    query.metalog_progress = result.metalog_progress;
    query.prev_found_result = result.found_result;
    return query;
}

IndexQueryResult Indexer::BuildIndexResult(protocol::SharedLogMessage message){
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    return IndexQueryResult {
        .state = message.found_seqnum == kInvalidLogSeqNum ? 
            IndexQueryResult::State::kEmpty : IndexQueryResult::State::kFound,
        .metalog_progress = message.user_metalog_progress,
        .original_query = IndexQuery {
            .direction = IndexQuery::DirectionFromOpType(op_type),
            .origin_node_id = message.engine_node_id,
            .hop_times = message.hop_times,
            .client_data = message.client_data,
            .user_logspace = message.user_logspace,
            .query_seqnum = message.query_seqnum,
            .metalog_progress = message.user_metalog_progress
        },
        .found_result = IndexFoundResult {
            .view_id = message.found_view_id,
            .storage_shard_id = message.found_storage_shard_id,
            .seqnum = message.found_seqnum
        }
    };
}

void Indexer::FlushIndexEntries() {
    //TODO: persist index data
}

}  // namespace log
}  // namespace faas
