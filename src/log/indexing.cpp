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

IndexNode::IndexNode(uint16_t node_id)
    : IndexBase(node_id),
      log_header_(fmt::format("Index[{}-N]: ", node_id)),
      current_view_(nullptr),
      view_finalized_(false),
      next_index_read_op_id_(0) {}

IndexNode::~IndexNode() {}

void IndexNode::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    bool contains_myself = view->contains_storage_node(my_node_id());
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
                // if (index_node->HasIndexFor(sequencer_id)) {
                index_collection_.InstallLogSpace(std::make_unique<Index>(
                    view, sequencer_id));
                // }
            }
        }
        future_requests_.OnNewView(view, contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        view_finalized_ = false;
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
    // HVLOG_F(1, "Handle index read: op_id={}, logspace={}, tag={}, seqnum={}",
    //         op->id, op->user_logspace, op->query_tag, bits::HexStr0x(op->seqnum));
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
        // uint32_t logspace_id = current_view_->LogSpaceIdentifier(request.user_logspace);
        // TODO: index nodes have all physical log spaces?
        index_ptr = index_collection_.GetLogSpaceChecked(request.logspace_id);
    }
    if (index_ptr != nullptr) {
        IndexQuery query = BuildIndexQuery(request);
        Index::QueryResultVec query_results;
        {
            auto locked_index = index_ptr.Lock();
            locked_index->MakeQuery(query);
            locked_index->PollQueryResults(&query_results);
        }
        ProcessIndexQueryResults(query_results);
    } else {
        HVLOG_F(1, "There is no local index for logspace {}", request.logspace_id);
        IndexQuery query = BuildIndexQuery(request);
        SendIndexReadFailureResponse(query, protocol::SharedLogResultType::NO_INDEX);
    }
}

void IndexNode::OnRecvNewIndexData(const SharedLogMessage& message,
                                     std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::INDEX_DATA);
    IndexDataProto index_data_proto;
    if (!index_data_proto.ParseFromArray(payload.data(),
                                         static_cast<int>(payload.size()))) {
        LOG(FATAL) << "Failed to parse IndexDataProto";
    }
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        DCHECK(message.view_id < views_.size());
        const View* view = views_.at(message.view_id);
        if (!view->contains_index_node(my_node_id())) {
            HLOG_F(FATAL, "View {} does not contain myself", view->id());
        }
        // const View::Index* index_node = view->GetIndexNode(my_node_id());
        //TODO: index nodes with no index for sequencer
        // if (!index_node->HasIndexFor(message.sequencer_id)) {
        //     HLOG_F(FATAL, "This node has no index for log space {}",
        //            bits::HexStr0x(message.logspace_id));
        // }
        auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_index = index_ptr.Lock();
            // store
            locked_index->ProvideIndexData(index_data_proto);
            locked_index->PollQueryResults(&query_results);
        }
    }
    ProcessIndexQueryResults(query_results);
}

bool IndexNode::MergeIndexResult(IndexQueryResult index_query_result_other, IndexQueryResult* merged_index_query_result){
    DCHECK_NE(index_query_result_other.state, IndexQueryResult::State::kContinue); //"kContinue cannot be merged"
    absl::MutexLock view_lk(&view_mu_);
    absl::MutexLock lk(&index_reads_mu_);
    IndexReadOp* op;
    bool exists = ongoing_index_reads_.contains(index_query_result_other.original_query.client_data);
    if (exists == false) {
        IndexReadOp* op = index_read_op_pool_.Get();
        op->id = next_index_read_op_id_.fetch_add(1, std::memory_order_acq_rel);
        op->start_timestamp = GetMonotonicMicroTimestamp();
        op->merged_counter = 1;
        op->index_query_result = index_query_result_other;
        ongoing_index_reads_.insert({index_query_result_other.original_query.client_data, op});
        return false;
    }
    op = ongoing_index_reads_.at(index_query_result_other.original_query.client_data);
    uint64_t mergedResult = op->index_query_result.found_result.seqnum;
    uint64_t slaveResult = index_query_result_other.found_result.seqnum;
    DCHECK_NE(mergedResult, slaveResult);
    if (op->index_query_result.state == IndexQueryResult::State::kEmpty){
        // other result is empty or found
        op->index_query_result.found_result = index_query_result_other.found_result;
    }
    else if (op->index_query_result.original_query.direction == IndexQuery::ReadDirection::kReadPrev){
        if (mergedResult < slaveResult) {
            // other result is closer
            op->index_query_result.found_result = index_query_result_other.found_result;
        }
    } else { // readNext, readNextB
        if (slaveResult < mergedResult) {
            // other result is closer
            op->index_query_result.found_result = index_query_result_other.found_result;
        }
    }
    op->merged_counter++;
    merged_index_query_result = &op->index_query_result;
    if(op->merged_counter == current_view_->num_index_shards()){
        ongoing_index_reads_.erase(op->index_query_result.original_query.client_data);
        return true;
    }
    return false;
}

void IndexNode::HandleSlaveResult(const protocol::SharedLogMessage& message, std::span<const char> payload){
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::INDEX_RESULT);
    IndexResultProto index_result_proto;
    if (!index_result_proto.ParseFromArray(payload.data(),
                                         static_cast<int>(payload.size()))) {
        LOG(FATAL) << "Failed to parse IndexFoundResultProto";
    }
    IndexQueryResult slave_index_query_result = BuildIndexResult(message, index_result_proto);
    IndexQueryResult merged_index_query_result;
    if(MergeIndexResult(slave_index_query_result, &merged_index_query_result)){
        if (merged_index_query_result.state == IndexQueryResult::kFound){
            ForwardReadRequest(merged_index_query_result);
        } else {
            SendIndexReadFailureResponse(merged_index_query_result.original_query, protocol::SharedLogResultType::NO_INDEX);
        }
    }
}


#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW
#undef RETURN_IF_LOGSPACE_FINALIZED

void IndexNode::ProcessIndexResult(const IndexQueryResult& query_result) {
    DCHECK(query_result.state == IndexQueryResult::kFound || query_result.state == IndexQueryResult::kEmpty);
    if (IsSlave(query_result.original_query)){
        SendMasterIndexResult(query_result);
        return;
    }
    IndexQueryResult merged_index_query_result;
    if(MergeIndexResult(query_result, &merged_index_query_result)){
        if (merged_index_query_result.state == IndexQueryResult::kFound){
            ForwardReadRequest(merged_index_query_result);
        } else {
            SendIndexReadFailureResponse(merged_index_query_result.original_query, protocol::SharedLogResultType::NO_INDEX);
        }
    }
}

void IndexNode::ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                        Index::QueryResultVec* more_results) {
    DCHECK(query_result.state == IndexQueryResult::kContinue);
    HVLOG_F(1, "Process IndexContinueResult: next_view_id={}",
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
        HLOG(ERROR) << "No index for logspace";
    }
}

void IndexNode::ForwardReadRequest(const IndexQueryResult& query_result){
    DCHECK(query_result.state == IndexQueryResult::kFound);
    const View::Engine* engine_node = nullptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        uint16_t view_id = query_result.found_result.view_id;
        if (view_id < views_.size()) {
            const View* view = views_.at(view_id);
            engine_node = view->GetEngineNode(query_result.found_result.engine_id);
        } else {
            HLOG_F(FATAL, "Cannot find view {}", view_id);
        }
    }
    bool success = SendStorageReadRequest(query_result, engine_node);
    if (!success) {
        uint64_t seqnum = query_result.found_result.seqnum;
        IndexQuery query = query_result.original_query;
        HLOG_F(WARNING, "Failed to send read request for seqnum {} ", bits::HexStr0x(seqnum));
        SendIndexReadFailureResponse(query, protocol::SharedLogResultType::DATA_LOST);
    }
}

void IndexNode::ProcessIndexQueryResults(const Index::QueryResultVec& results) {
    Index::QueryResultVec more_results;
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

void IndexNode::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_AS_SPAN(request.payload));
    }
}

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
    request.prev_engine_id = result.found_result.engine_id;
    request.prev_found_seqnum = result.found_result.seqnum;
    return request;
}

IndexQuery IndexNode::BuildIndexQuery(const SharedLogMessage& message) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    return IndexQuery {
        .direction = IndexQuery::DirectionFromOpType(op_type),
        .origin_node_id = message.origin_node_id,
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
            .engine_id = message.prev_engine_id,
            .seqnum = message.prev_found_seqnum
        },
    };
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
    IndexQuery query = BuildIndexQuery(message);
    return IndexQueryResult {
        .state = result.found() ? IndexQueryResult::State::kFound : IndexQueryResult::State::kEmpty,
        .metalog_progress = query.metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = gsl::narrow_cast<uint16_t>(result.view_id()),
            .engine_id = gsl::narrow_cast<uint16_t>(result.location_identifier()),
            .seqnum = result.seqnum()
        }
    };
}

}  // namespace log
}  // namespace faas
