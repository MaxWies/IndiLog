#include "log/merger.h"

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

Merger::Merger(uint16_t node_id)
    : MergerBase(node_id),
      log_header_(fmt::format("Index[{}-N]: ", node_id)),
      current_view_(nullptr),
      current_view_active_(false) {}

Merger::~Merger() {}

void Merger::OnViewCreated(const View* view) {
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
            }
        }
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

void Merger::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
}

void Merger::OnRecvRegistration(const protocol::SharedLogMessage& received_message) {
    DCHECK(SharedLogMessageHelper::GetOpType(received_message) == SharedLogOpType::REGISTER);
    DCHECK(SharedLogMessageHelper::GetResultType(received_message) == SharedLogResultType::REGISTER_ENGINE);
    absl::MutexLock view_lk(&view_mu_);
    if(received_message.view_id != current_view_->id()){
        HLOG_F(WARNING, "Current view not the same. register_view={}, my_current_view={}", received_message.view_id, current_view_->id());
        HLOG(WARNING) << "Registration failed";
        SharedLogMessage response = SharedLogMessageHelper::NewRegisterResponseMessage(
            SharedLogResultType::REGISTER_MERGER_FAILED,
            current_view_->id(),
            received_message.sequencer_id,
            received_message.storage_shard_id,
            received_message.engine_node_id,
            received_message.local_start_id
        );
        SendRegistrationResponse(received_message, &response);
        return;
    }
    if(ongoing_engine_index_reads_.contains(received_message.engine_node_id)){
        HLOG_F(WARNING, "Remove index read operations of engine with id={}", received_message.engine_node_id);
        ongoing_engine_index_reads_.erase(received_message.engine_node_id);
    }
    EngineIndexReadOp* engine_index_read_ops = new EngineIndexReadOp();
    ongoing_engine_index_reads_[received_message.engine_node_id].reset(engine_index_read_ops);
    HLOG_F(INFO, "Registration ok. engine_id={}", received_message.engine_node_id);
    SharedLogMessage response = SharedLogMessageHelper::NewRegisterResponseMessage(
            SharedLogResultType::REGISTER_MERGER_OK,
            received_message.view_id,
            received_message.sequencer_id,
            received_message.storage_shard_id,
            received_message.engine_node_id,
            received_message.local_start_id
    );
    SendRegistrationResponse(received_message, &response);
}

void Merger::RemoveEngineNode(uint16_t engine_node_id){
    absl::MutexLock view_lk(&view_mu_);
    if (ongoing_engine_index_reads_.contains(engine_node_id)){
        ongoing_engine_index_reads_.erase(engine_node_id);
    }
}

bool Merger::MergeIndexResult(const uint16_t index_node_id_other, const IndexQueryResult& index_query_result_other, IndexQueryResult* merged_index_query_result){
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
    return engine_index_read_op->Merge(num_index_shards, index_node_id_other, index_query_result_other, merged_index_query_result);
}

void Merger::HandleSlaveResult(const protocol::SharedLogMessage& message){
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::READ_NEXT_INDEX_RESULT || 
           SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::READ_PREV_INDEX_RESULT ||
           SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::READ_NEXT_B_INDEX_RESULT);
    IndexQueryResult slave_index_query_result = BuildIndexResult(message);
    IndexQueryResult merged_index_query_result;
    bool merge_complete = MergeIndexResult(message.origin_node_id, slave_index_query_result, &merged_index_query_result);
    if (merge_complete){
        if (merged_index_query_result.IsFound() && !merged_index_query_result.IsPointHit()){
            ForwardReadRequest(merged_index_query_result);
        } else if (!merged_index_query_result.IsFound()){
            SendIndexReadFailureResponse(merged_index_query_result.original_query, protocol::SharedLogResultType::EMPTY);
        }
    }
}

void Merger::ForwardReadRequest(const IndexQueryResult& query_result){
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

void Merger::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_AS_SPAN(request.payload));
    }
}

IndexQueryResult Merger::BuildIndexResult(protocol::SharedLogMessage message){
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

}  // namespace log
}  // namespace faas
