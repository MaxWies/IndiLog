#include "log/indexing.h"

#include "log/flags.h"
#include "log/utils.h"
#include "utils/bits.h"
#include "utils/io.h"
#include "utils/timerfd.h"

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

IndexNode::IndexNode(uint16_t node_id)
    : IndexBase(node_id),
      log_header_(fmt::format("Index[{}-N]: ", node_id)),
      current_view_(nullptr),
      current_view_active_(false),
      next_index_read_op_id_(0) {}

IndexNode::~IndexNode() {}

void IndexNode::OnViewCreated(const View* view) {
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
            //const View::Index* index_node = view->GetIndexNode(my_node_id());
            for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                if (!view->is_active_phylog(sequencer_id)) {
                    continue;
                }
                //TODO: currently all index nodes have index for sequencer
                HLOG_F(INFO, "Create logspace for view {} and sequencer {}", view->id(), sequencer_id);
                index_collection_.InstallLogSpace(std::make_unique<Index>(view, sequencer_id));
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

// void IndexNode::OnViewFrozen(const View* view) {
//     DCHECK(zk_session()->WithinMyEventLoopThread());
//     HLOG_F(INFO, "View {} frozen", view->id());
//     absl::MutexLock view_lk(&view_mu_);
//     DCHECK_EQ(view->id(), current_view_->id());
//     if (view->contains_engine_node(my_node_id())) {
//         DCHECK(current_view_active_);
//         current_view_active_ = false;
//     }
// }

void IndexNode::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    Index::QueryResultVec index_query_results;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        index_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &index_query_results] (uint32_t logspace_id,
                                              LockablePtr<Index> index_ptr) {
                log_utils::FinalizedLogSpace<Index>(
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

void IndexNode::HandleReadRequest(const SharedLogMessage& request) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(request);
    DCHECK(  op_type == SharedLogOpType::READ_NEXT
          || op_type == SharedLogOpType::READ_PREV
          || op_type == SharedLogOpType::READ_NEXT_B);
    DCHECK_EQ(request.use_master_node_id, protocol::kUseMasterNodeId);
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
        // uint32_t logspace_id = current_view_->LogSpaceIdentifier(request.user_logspace);
        // TODO: index nodes have all physical log spaces?
        index_ptr = index_collection_.GetLogSpaceChecked(request.logspace_id);
    }
    if (index_ptr != nullptr) {
        IndexQuery query = BuildIndexQuery(request, request.origin_node_id);
        HVLOG_F(1, "IndexRead: Make query. client_id={}, metalog={}, logspace={}, tag={}, query_seqnum={}", 
            request.client_data, bits::HexStr0x(query.metalog_progress), request.logspace_id, query.user_tag, bits::HexStr0x(query.query_seqnum)
        );
        Index::QueryResultVec query_results;
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

void IndexNode::HandleReadMinRequest(const SharedLogMessage& request) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(request);
    DCHECK(op_type == SharedLogOpType::READ_MIN);
    IndexQuery query = BuildIndexQuery(request, request.origin_node_id);
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
        index_ptr = index_collection_.GetLogSpaceChecked(request.logspace_id);
    }
    if (index_ptr != nullptr) {
        HVLOG_F(1, "IndexRead: Make query for min seqnum. seqnum={}, logspace={}, tag={}", bits::HexStr0x(query.query_seqnum), request.logspace_id, query.user_tag);
        Index::QueryResultVec query_results;
        {
            auto locked_index = index_ptr.Lock();
            locked_index->MakeQuery(query);
            locked_index->PollQueryResults(&query_results);
        }
        ProcessIndexQueryResults(query_results);
    } else {
        HVLOG_F(1, "IndexRead: There is no local index for logspace {}", request.logspace_id);
        SendIndexReadFailureResponse(query, protocol::SharedLogResultType::INDEX_MIN_FAILED);
    }
}

void IndexNode::OnRecvNewIndexData(const SharedLogMessage& message,
                                     std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::INDEX_DATA);
    HVLOG_F(1, "IndexUpdate: Index data received from storage_node={}", message.origin_node_id);
    IndexDataProto index_data_proto;
    if (!index_data_proto.ParseFromArray(payload.data(),
                                         static_cast<int>(payload.size()))) {
        LOG(FATAL) << "IndexUpdate: Failed to parse IndexDataProto";
    }

    if(index_data_proto.meta_headers_size() < 1){
        LOG(WARNING) << "IndexUpdate: IndexDataProto without any metaheaders";
        return;
    }

    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        DCHECK(message.view_id < views_.size());
        const View* view = views_.at(message.view_id);
        if (!view->contains_index_node(my_node_id())) {
            HLOG_F(FATAL, "IndexUpdate: View {} does not contain myself", view->id());
        }
        //TODO: is index check
        const View::Storage* storage_node = view->GetStorageNode(message.origin_node_id);
        View::NodeIdVec storage_shards = storage_node->GetLocalStorageShardIds(message.sequencer_id);

        auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_index = index_ptr.Lock();
            if(!locked_index->AdvanceIndexProgress(index_data_proto, my_node_id())){
                return;
            }
            locked_index->PollQueryResults(&query_results);
        }
    }
    ProcessIndexQueryResults(query_results);
}

void IndexNode::OnRecvRegistration(const protocol::SharedLogMessage& received_message) {
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
            received_message.shard_id,
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
    HLOG_F(INFO, "Registration ok. engine_id={}", received_message.engine_node_id);
    SharedLogMessage response = SharedLogMessageHelper::NewRegisterResponseMessage(
            SharedLogResultType::REGISTER_INDEX_OK,
            received_message.view_id,
            received_message.sequencer_id,
            received_message.shard_id,
            received_message.engine_node_id,
            received_message.local_start_id
    );
    SendRegistrationResponse(received_message, &response);
}

void IndexNode::RemoveEngineNode(uint16_t engine_node_id){
    absl::MutexLock view_lk(&view_mu_);
    view_mutable_.RemoveCurrentEngineNodeId(engine_node_id);
}

bool IndexNode::MergeIndexResult(const uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* merged_index_query_result){
    bool isMyResult = index_node_id_other == my_node_id();
    HVLOG_F(1, "IndexRead: Index result received. is_my_result={}, index_node={}, engine_node={}, client_key={}, query_seqnum={}", 
        isMyResult, index_node_id_other, index_query_result_other.original_query.origin_node_id, bits::HexStr0x(index_query_result_other.original_query.client_data), 
        bits::HexStr0x(index_query_result_other.original_query.query_seqnum)
    );
    DCHECK_NE(index_query_result_other.state, IndexQueryResult::kContinue); //"kContinue cannot be merged"
    absl::MutexLock view_lk(&view_mu_);
    absl::MutexLock lk(&index_reads_mu_);
    IndexReadOp* op;
    std::pair<uint16_t, uint64_t> key = std::make_pair(index_query_result_other.original_query.origin_node_id, index_query_result_other.original_query.client_data);
    bool exists = ongoing_index_reads_.contains(key);
    if (!exists) {
        op = index_read_op_pool_.Get();
        op->id = next_index_read_op_id_.fetch_add(1, std::memory_order_acq_rel);
        op->start_timestamp = GetMonotonicMicroTimestamp();
        op->merged_nodes.insert(index_node_id_other);
        op->index_query_result = index_query_result_other;
        ongoing_index_reads_.insert({key, op});
        HVLOG_F(1, "IndexRead: Create new index read operation with op_id={}", op->id);
    } else {
        op = ongoing_index_reads_.at(key);
        HVLOG_F(1, "IndexRead: Retrieve index read operation with op_id={}", op->id);
        if(op->merged_nodes.contains(index_node_id_other)){
            HLOG_F(ERROR, "IndexRead: Result of index_node={} was already merged for op_id={}", index_node_id_other, op->id);
            return false;
        }
        uint64_t mergedResult = op->index_query_result.found_result.seqnum;
        uint64_t slaveResult = index_query_result_other.found_result.seqnum;
        HVLOG_F(1, "IndexRead: Merging: merged_result={}, other_result={}, op_id={}", mergedResult, slaveResult, op->id);
        if (op->index_query_result.state == IndexQueryResult::kFound && index_query_result_other.state == IndexQueryResult::kFound){
            if (mergedResult == slaveResult) {
                LOG(FATAL) << "Due to sharding there can never be the same result if sequence numbers were found";
            }
        }
        if (op->index_query_result.state == IndexQueryResult::kFound && index_query_result_other.state == IndexQueryResult::kEmpty){
            // merged result is found, other result is empty
            HVLOG (1) << "IndexRead: Current result is FOUND, other result is EMPTY";
        } 
        else if (op->index_query_result.state == IndexQueryResult::kEmpty && index_query_result_other.state == IndexQueryResult::kFound) {
            // merged result is empty, other result is found
            HVLOG (1) << "IndexRead: Current result is EMPTY, other result is FOUND";
            op->index_query_result = index_query_result_other;
        }
        else if (op->index_query_result.original_query.direction == IndexQuery::ReadDirection::kReadPrev){
            if (mergedResult < slaveResult) {
                // other result is closer
                HVLOG_F(1, "IndexRead: Current result is FOUND({}), other result is FOUND({}). Other is closer for read_prev and query_seqnum={}", 
                    mergedResult, slaveResult, bits::HexStr0x(op->index_query_result.original_query.query_seqnum)
                );
                op->index_query_result = index_query_result_other;
            }
        } 
        else { // readNext, readNextB
            if (slaveResult < mergedResult) {
                // other result is closer
                HVLOG_F(1, "IndexRead: Current result is FOUND({}), other result is FOUND({}). Other is closer for read_next and query_seqnum={}", 
                    mergedResult, slaveResult, bits::HexStr0x(op->index_query_result.original_query.query_seqnum)
                );
                op->index_query_result = index_query_result_other;
            }
        }
        op->merged_nodes.insert(index_node_id_other);
        HVLOG_F(1, "IndexRead: Merged result merged={} for op_id={}", op->merged_nodes.size(), op->id);
    }
    *merged_index_query_result = std::move(op->index_query_result);
    if(op->merged_nodes.size() == current_view_->num_index_shards()){
        ongoing_index_reads_.erase(key);
        return true;
    }
    DCHECK_LT(op->merged_nodes.size(), current_view_->num_index_shards());
    return false;
}

void IndexNode::HandleSlaveResult(const protocol::SharedLogMessage& message, std::span<const char> payload){
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::READ_NEXT_INDEX_RESULT || 
           SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::READ_PREV_INDEX_RESULT ||
           SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::READ_NEXT_B_INDEX_RESULT);
    IndexResultProto index_result_proto;
    if (!index_result_proto.ParseFromArray(payload.data(),
                                         static_cast<int>(payload.size()))) {
        LOG(FATAL) << "IndexRead: Failed to parse IndexFoundResultProto";
    }
    IndexQueryResult slave_index_query_result = BuildIndexResult(message, index_result_proto);
    IndexQueryResult merged_index_query_result;
    bool merge_complete = MergeIndexResult(message.origin_node_id, slave_index_query_result, &merged_index_query_result);
    if (merge_complete && merged_index_query_result.IsFound() && !merged_index_query_result.IsPointHit()){
        HVLOG_F(1, "IndexRead: Index result found in index tier. Forward read request for query_seqnum={}", bits::HexStr0x(merged_index_query_result.original_query.query_seqnum));
        ForwardReadRequest(merged_index_query_result);
    } else if (merge_complete && !merged_index_query_result.IsFound()){
        HLOG_F(INFO, "IndexRead: No shard was able to find a result for rd={}, logspace={}, tag={}, query_seqnum={}",
            merged_index_query_result.original_query.DirectionToString(), 
            merged_index_query_result.original_query.user_logspace, 
            merged_index_query_result.original_query.user_tag, 
            bits::HexStr0x(merged_index_query_result.original_query.query_seqnum)
        );
        SendIndexReadFailureResponse(merged_index_query_result.original_query, protocol::SharedLogResultType::EMPTY);
    }
}


#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW
#undef RETURN_IF_LOGSPACE_FINALIZED

void IndexNode::ProcessIndexResult(const IndexQueryResult& my_query_result) {
    if (my_query_result.IsPointHit()){
        HVLOG_F(1, "IndexRead: Point hit. Forward read request for query_seqnum={}", bits::HexStr0x(my_query_result.original_query.query_seqnum));
        ForwardReadRequest(my_query_result);
    }
    if (IsSlave(my_query_result.original_query)){
        HVLOG_F(1, "IndexRead: I am slave. Will send to master node {}", my_node_id(), my_query_result.original_query.master_node_id);
        SendMasterIndexResult(my_query_result);
        return;
    }
    IndexQueryResult merged_index_query_result;
    bool merge_complete = MergeIndexResult(my_node_id(), my_query_result, &merged_index_query_result);
    if (merge_complete && merged_index_query_result.IsFound() && !merged_index_query_result.IsPointHit()) {
        HVLOG_F(1, "IndexRead: Index result found in index tier. Forward read request for query_seqnum={}", bits::HexStr0x(merged_index_query_result.original_query.query_seqnum));
        ForwardReadRequest(merged_index_query_result);
    } else if (merge_complete && !merged_index_query_result.IsFound()){
        HLOG_F(INFO, "IndexRead: No shard was able to find a result for rd={}, logspace={}, tag={}, query_seqnum={}",
            merged_index_query_result.original_query.DirectionToString(), 
            merged_index_query_result.original_query.user_logspace, 
            merged_index_query_result.original_query.user_tag, 
            bits::HexStr0x(merged_index_query_result.original_query.query_seqnum)
        );
        SendIndexReadFailureResponse(merged_index_query_result.original_query, protocol::SharedLogResultType::EMPTY);
    }
}

void IndexNode::ProcessIndexMinResult(const IndexQueryResult& query_result) {
    if (query_result.original_query.min_seqnum_query){
        if (query_result.found_result.seqnum == kInvalidLogSeqNum) {
            // broadcast new tag
            uint32_t logspace_id;
            std::vector<uint16_t> engine_ids;
            {
                absl::ReaderMutexLock view_lk(&view_mu_);
                logspace_id = current_view_->LogSpaceIdentifier(query_result.original_query.user_logspace);
                uint16_t seqnum_id = bits::LowHalf32(logspace_id);
                for (uint16_t engine_node_id : view_mutable_.GetCurrentEngineNodeIds(seqnum_id)){
                    engine_ids.push_back(engine_node_id);
                }
            }
            HVLOG_F(1, "Tag={} is new. Broadcast to {} nodes on compute tier", query_result.original_query.user_tag, engine_ids.size());
            BroadcastIndexReadResponse(query_result, engine_ids, logspace_id);
        } else {
            // send response to engine node
            HVLOG_F(1, "Tag={} exists with min_seqnum={}. Send to engine_node={}", 
                query_result.original_query.user_tag, query_result.found_result.seqnum, query_result.original_query.origin_node_id
            );
            uint32_t logspace_id;
            {
                absl::ReaderMutexLock view_lk(&view_mu_);
                logspace_id = current_view_->LogSpaceIdentifier(query_result.original_query.user_logspace);
            }
            SendIndexReadResponse(query_result, logspace_id);
        }
        return;
    }
}

void IndexNode::ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                        Index::QueryResultVec* more_results) {
    DCHECK(query_result.state == IndexQueryResult::kContinue);
    HVLOG_F(1, "IndexRead: Process IndexContinueResult: next_view_id={}",
            query_result.next_view_id);
    const IndexQuery& query = query_result.original_query;
    //const View::Sequencer* sequencer_node = nullptr;
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        uint16_t view_id = query_result.next_view_id;
        if (view_id >= views_.size()) {
            HLOG_F(FATAL, "Cannot find view {}", view_id);
        }
        const View* view = views_.at(view_id);
        uint32_t logspace_id = view->LogSpaceIdentifier(query.user_logspace);
        // TODO: index node has index for all sequencers?
        // sequencer_node = view->GetSequencerNode(bits::LowHalf32(logspace_id));
        // if (sequencer_node->IsIndexEngineNode(my_node_id())) {
        //     index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
        // }
        index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
    }
    if (index_ptr != nullptr) {
        IndexQuery query = BuildIndexQuery(query_result);
        auto locked_index = index_ptr.Lock();
        locked_index->MakeQuery(query);
        locked_index->PollQueryResults(more_results);
    } else {
        //uint32_t logspace_id = view->LogSpaceIdentifier(query.user_logspace);
        HLOG(ERROR) << "IndexRead: No index for logspace";
    }
}

void IndexNode::ForwardReadRequest(const IndexQueryResult& query_result){
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
    if (success) {
        //Todo: a bit shady here
        SendIndexReadResponse(query_result, logspace_id);
    }
    if (!success) {
        uint64_t seqnum = query_result.found_result.seqnum;
        IndexQuery query = query_result.original_query;
        HLOG_F(WARNING, "IndexRead: Failed to send read request for seqnum {} ", bits::HexStr0x(seqnum));
        SendIndexReadFailureResponse(query, protocol::SharedLogResultType::DATA_LOST);
    }
}

void IndexNode::ProcessIndexQueryResults(const Index::QueryResultVec& results) {
    Index::QueryResultVec more_results;
    for (const IndexQueryResult& result : results) {
        switch (result.state) {
        case IndexQueryResult::kEmpty:
        case IndexQueryResult::kFound:
            result.original_query.min_seqnum_query ? ProcessIndexMinResult(result) : ProcessIndexResult(result);
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

void IndexNode::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_AS_SPAN(request.payload));
    }
}

// not used so far
SharedLogMessage IndexNode::BuildReadRequestMessage(const IndexQueryResult& result) {
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

IndexQuery IndexNode::BuildIndexQuery(const SharedLogMessage& message, const uint16_t original_requester_id) {
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
        .master_node_id = message.master_node_id,
        .prev_found_result = IndexFoundResult {
            .view_id = message.prev_view_id,
            .storage_shard_id = message.prev_shard_id,
            .seqnum = message.prev_found_seqnum
        },
    };
    if(message.use_master_node_id == protocol::kUseMasterNodeId){
        index_query.master_node_id = message.master_node_id;
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
    if (op_type == SharedLogOpType::READ_MIN) {
        index_query.min_seqnum_query = true;
        index_query.tail_seqnum = message.tail_seqnum;
        index_query.prev_found_result = IndexFoundResult {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = 0
        };
    }
    return index_query;
}

IndexQuery IndexNode::BuildIndexQuery(const IndexQueryResult& result) {
    DCHECK(result.state == IndexQueryResult::kContinue);
    IndexQuery query = result.original_query;
    query.initial = false;
    query.metalog_progress = result.metalog_progress;
    query.prev_found_result = result.found_result;
    return query;
}

// void IndexNode::BackgroundThreadMain() {
//     int timerfd = io_utils::CreateTimerFd();
//     CHECK(timerfd != -1) << "Failed to create timerfd";
//     io_utils::FdUnsetNonblocking(timerfd);
//     absl::Duration interval = absl::Milliseconds(
//         absl::GetFlag(FLAGS_slog_index_bgthread_interval_ms));
//     CHECK(io_utils::SetupTimerFdPeriodic(timerfd, absl::Milliseconds(100), interval))
//         << "Failed to setup timerfd with interval " << interval;
//     bool running = true;
//     while (running) {
//         uint64_t exp;
//         ssize_t nread = read(timerfd, &exp, sizeof(uint64_t));
//         if (nread < 0) {
//             PLOG(FATAL) << "Failed to read on timerfd";
//         }
//         CHECK_EQ(gsl::narrow_cast<size_t>(nread), sizeof(uint64_t));
//         // TODO: flushing on index tier
//         // FlushLogEntries();
//         // TODO: cleanup outdated LogSpace
//         running = state_.load(std::memory_order_acquire) != kStopping;
//     }
// }

void IndexNode::FlushIndexEntries() {
    // TODO: flushing
}

IndexQueryResult IndexNode::BuildIndexResult(protocol::SharedLogMessage message, IndexResultProto result){
    IndexQuery query = BuildIndexQuery(message, gsl::narrow_cast<uint16_t>(result.original_requester_id()));
    return IndexQueryResult {
        .state = result.found() ? IndexQueryResult::State::kFound : IndexQueryResult::State::kEmpty,
        .metalog_progress = query.metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = gsl::narrow_cast<uint16_t>(result.view_id()),
            .storage_shard_id = gsl::narrow_cast<uint16_t>(result.storage_shard_id()),
            .seqnum = result.seqnum()
        }
    };
}

}  // namespace log
}  // namespace faas
