#include "log/engine.h"

#ifdef __FAAS_STAT_THREAD
#include <iostream>
#include <fstream>
#include "utils/timerfd.h"
#endif 

#include "engine/engine.h"
#include "log/flags.h"
#include "utils/bits.h"
#include "utils/random.h"
#include "server/constants.h"

#include "log/utils.h"

namespace faas {
namespace log {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

using server::IOWorker;
using server::ConnectionBase;
using server::IngressConnection;
using server::EgressHub;
using server::NodeWatcher;

using log::IndexingStrategy;

Engine::Engine(engine::Engine* engine)
    : EngineBase(engine),
      log_header_(fmt::format("LogEngine[{}-N]: ", my_node_id())),
      current_view_(nullptr),
      current_view_active_(false)
#ifdef __FAAS_STAT_THREAD
      ,
      statistics_thread_("BG_ST", [this] { this->StatisticsThreadMain(); }),
      statistics_thread_started_(false)
#endif
#ifdef __FAAS_OP_STAT
      ,
      append_ops_counter_(0),
      read_ops_counter_(0),
      local_index_hit_counter_(0),
      local_index_miss_counter_(0),
      index_min_read_ops_counter_(0),
      log_cache_hit_counter_(0),
      log_cache_miss_counter_(0) 
#endif
      {
          if(absl::GetFlag(FLAGS_slog_engine_index_tier_only)){
              indexing_strategy_ = IndexingStrategy::INDEX_TIER_ONLY;
          } else{
              if(absl::GetFlag(FLAGS_slog_engine_distributed_indexing)){
                  indexing_strategy_ = IndexingStrategy::DISTRIBUTED;
                  seqnum_cache_.emplace(1000);
              } else {
                  indexing_strategy_ = IndexingStrategy::COMPLETE;
              }
          }
      }

Engine::~Engine() {}

void Engine::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    {
        absl::MutexLock view_lk(&view_mu_);
        for (uint16_t sequencer_id : view->GetSequencerNodes()) {
            if (!view->is_active_phylog(sequencer_id)) {
                continue;
            }
            view_mutable_.CreateEngineConnection(
                bits::JoinTwo16(view->id(), sequencer_id), 
                view->num_storage_nodes(),
                view->num_index_nodes()
            );
            // apply for shard
            std::string znode_path_shard_req = fmt::format("storage_shard_req/{}_{}_{}", view->id(), sequencer_id, my_node_id());
            std::string znode_path_shard_resp = fmt::format("storage_shard_resp/{}_{}_{}", view->id(), sequencer_id, my_node_id());
            auto status = zk_utils::CreateSync(
                zk_session(), znode_path_shard_req, "", zk::ZKCreateMode::kEphemeral, nullptr
            );
            CHECK(status.ok()) << fmt::format("Failed to create ZooKeeper node {}: {}",
                                      znode_path_shard_req, status.ToString());
            std::string value;
            zk_utils::GetOrWaitSync(zk_session(), znode_path_shard_resp, &value);
            HVLOG_F(1, "Received from znode {} content {}", znode_path_shard_resp, value);
            std::string_view prefix;
            if (absl::StartsWith(value, "ok:")){
                prefix = "ok:";
            } else if (absl::StartsWith(value, "err:")){
                HLOG_F(ERROR, "Getting shard id failed for log {}", sequencer_id);
                return;
            } else {
                UNREACHABLE();
            }
            int parsed;
            uint16_t shard_id;
            if (!absl::SimpleAtoi(absl::StripPrefix(value, prefix), &parsed)) {
                HLOG(ERROR) << "Failed to parse value: " << value;
                return;
            }
            shard_id = gsl::narrow_cast<uint16_t>(parsed);
            HLOG_F(INFO, "Received shard id {} from controller for sequencer {}. Start registration", shard_id, sequencer_id);
            // clean zk
            zk_utils::DeleteSync(zk_session(), znode_path_shard_resp);
            SharedLogMessage request = protocol::SharedLogMessageHelper::NewRegisterMessage(
                view->id(),
                sequencer_id,
                shard_id,
                my_node_id()
            );
            SendRegistrationRequest(sequencer_id, protocol::ConnType::ENGINE_TO_SEQUENCER, &request);
        }
        current_view_ = view;
        views_.push_back(view);
        log_header_ = fmt::format("LogEngine[{}-{}]: ", my_node_id(), view->id());
    }
}

#ifdef __FAAS_STAT_THREAD
void Engine::OnActivateStatisticsThread() {
    if (!statistics_thread_started_){
        statistics_thread_.Start();
        statistics_thread_started_ = true;   
    }
}
#endif

void Engine::OnViewFrozen(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} frozen", view->id());
    absl::MutexLock view_lk(&view_mu_);
    DCHECK_EQ(view->id(), current_view_->id());
    std::vector<uint16_t> active_sequencer_nodes;
    view->GetActiveSequencerNodes(&active_sequencer_nodes);
    if (view_mutable_.IsEngineActive(view->id(), active_sequencer_nodes)) {
        DCHECK(current_view_active_);
        current_view_active_ = false;
    }
}

void Engine::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    Index::QueryResultVec local_index_misses;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        producer_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &append_results] (uint32_t logspace_id,
                                               LockablePtr<LogProducer> producer_ptr) {
                log_utils::FinalizedLogSpace<LogProducer>(
                    producer_ptr, finalized_view);
                auto locked_producer = producer_ptr.Lock();
                LogProducer::AppendResultVec tmp;
                locked_producer->PollAppendResults(&tmp);
                append_results.insert(append_results.end(), tmp.begin(), tmp.end());
            }
        );
        switch(indexing_strategy_){
            case IndexingStrategy::DISTRIBUTED:
                // todo: broken
                // tag_cache_collection_.ForEachLogSpace(
                //     [finalized_view, &query_results] (uint32_t logspace_id,
                //                                     LockablePtr<TagCache> tag_cache_ptr) {
                //         log_utils::FinalizedLogSpace<TagCache>(
                //             tag_cache_ptr, finalized_view);
                //         auto locked_index = tag_cache_ptr.Lock();
                //         locked_index->PollQueryResults(&query_results);
                //     }
                // );
                break;
            case IndexingStrategy::COMPLETE:
                index_collection_.ForEachActiveLogSpace(
                    finalized_view->view(),
                    [finalized_view, &query_results] (uint32_t logspace_id,
                                                    LockablePtr<Index> index_ptr) {
                        log_utils::FinalizedLogSpace<Index>(
                            index_ptr, finalized_view);
                        auto locked_index = index_ptr.Lock();
                        locked_index->PollQueryResults(&query_results);
                    }
                );
                break;
            default:
                break;
        }
    }
    if (!append_results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, results = std::move(append_results)] {
                ProcessAppendResults(results);
            }
        );
    }
    if (!query_results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, results = std::move(query_results), &local_index_misses] {
                ProcessIndexQueryResults(results, &local_index_misses);
            }
        );
    }
    if (!local_index_misses.empty()) {
        absl::ReaderMutexLock view_lk(&view_mu_);
        auto my_storage_shards = view_mutable_.GetMyStorageShards();
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, misses = std::move(local_index_misses), finalized_view = finalized_view, storage_shards = my_storage_shards] 
            {
                for(const IndexQueryResult& miss : misses){
                    LocalOp* op = onging_reads_.GetChecked(miss.original_query.client_data);
                    uint32_t logspace_identifier = finalized_view->view()->LogSpaceIdentifier(op->user_logspace);
                    if(!storage_shards.contains(logspace_identifier)){
                        onging_reads_.RemoveChecked(op->id);
                        FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
                        continue;
                    }
                    const View::StorageShard* shard = storage_shards.at(logspace_identifier);
                    HandleIndexTierRead(op, finalized_view->view()->id(), shard);
                }
            }
        );
    }
}

namespace {
static Message BuildLocalReadOKResponse(uint64_t seqnum,
                                        std::span<const uint64_t> user_tags,
                                        std::span<const char> log_data) {
    Message response = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::READ_OK, seqnum);
    if (user_tags.size() * sizeof(uint64_t) + log_data.size() > MESSAGE_INLINE_DATA_SIZE) {
        LOG_F(FATAL, "Log data too large: num_tags={}, size={}",
              user_tags.size(), log_data.size());
    }
    response.log_num_tags = gsl::narrow_cast<uint16_t>(user_tags.size());
    MessageHelper::AppendInlineData(&response, user_tags);
    MessageHelper::AppendInlineData(&response, log_data);
    return response;
}

static Message BuildLocalReadOKResponse(const LogEntry& log_entry) {
    return BuildLocalReadOKResponse(
        log_entry.metadata.seqnum,
        VECTOR_AS_SPAN(log_entry.user_tags),
        STRING_AS_SPAN(log_entry.data));
}
}  // namespace

// Start handlers for local requests (from functions)

#define IGNORE_IF_NO_CONNECTION_FOR_LOGSPACE(LOGSPACE_ID)                       \
    do {                                                             \
        if (!view_mutable_.GetMyStorageShards().contains(LOGSPACE_ID)) { \
            HLOG(INFO) << "No connection to logspace " << LOGSPACE_ID;                  \
            return;                                                  \
        }                                                            \
    } while (0)

#define ONHOLD_IF_SEEN_FUTURE_VIEW(LOCAL_OP_VAR)                          \
    do {                                                                  \
        uint16_t view_id = log_utils::GetViewId(                          \
            (LOCAL_OP_VAR)->metalog_progress);                            \
        if (current_view_ == nullptr || view_id > current_view_->id()) {  \
            future_requests_.OnHoldRequest(                               \
                view_id, SharedLogRequest(LOCAL_OP_VAR));                 \
            return;                                                       \
        }                                                                 \
    } while (0)

void Engine::HandleLocalAppend(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::APPEND);
    HVLOG_F(1, "Handle local append: op_id={}, logspace={}, num_tags={}, size={}",
            op->id, op->user_logspace, op->user_tags.size(), op->data.length());
#ifdef __FAAS_OP_TRACING
    SaveTracePoint(op->id, "HandleLocalAppend");
#endif
    const View* view = nullptr;
    const View::StorageShard* storage_shard = nullptr;
    LogMetaData log_metadata = MetaDataFromAppendOp(op);
    uint64_t next_seqnum;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (!current_view_active_) {
            HLOG(WARNING) << "Current view not active";
            FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
            return;
        }
        view = current_view_;
        uint32_t logspace_id = view->LogSpaceIdentifier(op->user_logspace);
        storage_shard = view_mutable_.GetMyStorageShard(logspace_id);
        if (storage_shard == nullptr){
            HLOG_F(WARNING, "No storage shard for logspace={}", logspace_id);
            return;
        }
        log_metadata.seqnum = bits::JoinTwo32(logspace_id, 0);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            locked_producer->LocalAppend(op, &log_metadata.localid, &next_seqnum);
#ifdef __FAAS_OP_TRACING
            SaveTracePoint(op->id, "AfterPutToPendingList");
#endif
        }
    }
#ifdef __FAAS_OP_STAT
    append_ops_counter_.fetch_add(1, std::memory_order_acq_rel);
#endif
    if (indexing_strategy_ != IndexingStrategy::DISTRIBUTED 
        || absl::GetFlag(FLAGS_slog_deactivate_min_seqnum_completion) 
        || op->user_tags.empty() 
        || (op->user_tags.size() == 1 && op->user_tags.at(0) == kEmptyLogTag)) {
        ReplicateLogEntry(view, storage_shard, log_metadata, VECTOR_AS_SPAN(op->user_tags), op->data.to_span());
    } else {
        std::vector<uint64_t> tags_without_min_seqnum;
        {
            absl::ReaderMutexLock view_lk(&view_mu_);
            uint32_t logspace_id = view->LogSpaceIdentifier(op->user_logspace);
            auto tag_cache_ptr = tag_cache_collection_.GetLogSpaceChecked(bits::LowHalf32(logspace_id));
            {
                auto tag_cache = tag_cache_ptr.Lock();
                for (uint64_t tag : op->user_tags) {
                    if (!tag_cache->TagExists(op->user_logspace, tag)){
                        tags_without_min_seqnum.push_back(tag);
                    }
                }
            }
        }
        for (uint64_t tag : tags_without_min_seqnum) {
            HVLOG_F(1, "No seqnum for tag={}. Send index request", tag);
            HandleIndexTierMinSeqnumRead(op, tag, view->id(), std::min(uint64_t(0), next_seqnum - 1), storage_shard); // next_seqnum - 1 is the tail
        }
        ReplicateLogEntry(view, storage_shard, log_metadata, VECTOR_AS_SPAN(op->user_tags), op->data.to_span());
    }
}

void Engine::HandleLocalTrim(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::TRIM);
    NOT_IMPLEMENTED();
}

void Engine::HandleIndexTierRead(LocalOp* op, uint16_t view_id, const View::StorageShard* storage_shard){
    HVLOG(1) << "Send request to index tier";
    std::vector<uint16_t> index_nodes;
    storage_shard->PickIndexNodePerShard(index_nodes);
    uint16_t master_index_node = index_nodes.at(0);
    SharedLogMessage request = BuildIndexTierReadRequestMessage(op, master_index_node);
    request.sequencer_id = bits::HighHalf32(storage_shard->shard_id());
    request.view_id = view_id;
    bool send_success = true;
    std::ostringstream os;
    for(uint16_t index_node : index_nodes){
        send_success &= SendIndexTierReadRequest(index_node, &request);
        os << index_node << ",";
    }
    std::string index_nodes_str(os.str());
    if (!send_success) {
        HLOG_F(WARNING, "Failed to send index tier request. Index nodes {}. Master {}", index_nodes_str, master_index_node);
        onging_reads_.RemoveChecked(op->id);
        FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
        return;
    }
#ifdef __FAAS_OP_STAT
    local_index_miss_counter_.fetch_add(1, std::memory_order_acq_rel);
#endif 
#ifdef __FAAS_OP_TRACING
    SaveTracePoint(op->id, "SentIndexTierReadRequest");
#endif
    HVLOG_F(1, "Sent request to index tier successfully. Index nodes {}. Master {}", index_nodes_str, master_index_node);
    return;
}

void Engine::HandleIndexTierMinSeqnumRead(LocalOp* op, uint64_t tag, uint16_t view_id, uint64_t log_tail_seqnum, const View::StorageShard* storage_shard) {
    HVLOG(1) << "Send seqnum min request to index tier";
    uint16_t index_node = storage_shard->PickIndexNodeByTag(tag);
    SharedLogMessage request = BuildIndexTierMinSeqnumRequestMessage(op, tag, log_tail_seqnum);
    request.sequencer_id = bits::HighHalf32(storage_shard->shard_id());
    request.view_id = view_id;
    bool send_success = SendIndexTierReadRequest(index_node, &request);
    if (!send_success) {
        HLOG_F(WARNING, "Failed to send index tier request for min seqnum of tag {}", tag);
        return;
    }
#ifdef __FAAS_OP_STAT
    index_min_read_ops_counter_.fetch_add(1, std::memory_order_acq_rel);
#endif 
    HVLOG_F(1, "Sent request to index tier for min seqnum of tag {} successfully", tag);
}

void Engine::HandleLocalRead(LocalOp* op) {
    DCHECK(  op->type == SharedLogOpType::READ_NEXT
          || op->type == SharedLogOpType::READ_PREV
          || op->type == SharedLogOpType::READ_NEXT_B);
    HVLOG_F(1, "Handle local read: op_id={}, logspace={}, tag={}, seqnum={}",
            op->id, op->user_logspace, op->query_tag, bits::HexStr0x(op->seqnum));
#ifdef __FAAS_OP_TRACING
    SaveTracePoint(op->id, "HandleLocalRead");
#endif
    onging_reads_.PutChecked(op->id, op);
    uint32_t logspace_id;
    uint16_t view_id;
    const View::StorageShard* storage_shard = nullptr;
    LockablePtr<SeqnumSuffixChain> suffix_chain_ptr;
    LockablePtr<TagCache> tag_cache_ptr;
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_SEEN_FUTURE_VIEW(op);
        logspace_id = current_view_->LogSpaceIdentifier(op->user_logspace);
        if(view_mutable_.GetMyStorageShards().contains(logspace_id)){
            storage_shard = view_mutable_.GetMyStorageShard(logspace_id);
        }
        view_id = current_view_->id();
        if (indexing_strategy_ == IndexingStrategy::DISTRIBUTED) {
            suffix_chain_ptr = suffix_chain_collection_.GetLogSpaceChecked(bits::LowHalf32(logspace_id)); //todo: safe for view change?
            tag_cache_ptr = tag_cache_collection_.GetLogSpaceChecked(bits::LowHalf32(logspace_id));
        } else if (indexing_strategy_ == IndexingStrategy::COMPLETE) {
            index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
        }
    }
    if (storage_shard == nullptr){
        onging_reads_.RemoveChecked(op->id);
        HLOG(ERROR) << "No storage shard for current view";
        FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
        return;
    }
#ifdef __FAAS_OP_STAT
    read_ops_counter_.fetch_add(1, std::memory_order_acq_rel);
#endif 
    if (indexing_strategy_ == IndexingStrategy::INDEX_TIER_ONLY){
        HandleIndexTierRead(op, view_id, storage_shard);
        return;
    }
    IndexQuery query = BuildIndexQuery(op);
    if (indexing_strategy_ == IndexingStrategy::DISTRIBUTED) {
        Index::QueryResultVec local_index_misses;
        // use seqnum suffix
        if(op->query_tag == kEmptyLogTag){
            Index::QueryResultVec query_results;
            {
                auto locked_suffix_chain = suffix_chain_ptr.Lock();
                locked_suffix_chain->MakeQuery(query);
                locked_suffix_chain->PollQueryResults(&query_results);
            }
            ProcessIndexQueryResults(query_results, &local_index_misses);
            if (local_index_misses.size() < 1){
                return;
            }
            // use seqnum cache
            query_results.clear();
            for(IndexQueryResult result : local_index_misses){
                query_results.push_back(seqnum_cache_->MakeQuery(result.original_query));
            }
            local_index_misses.clear();
            ProcessIndexQueryResults(query_results, &local_index_misses);
        }
        // use tag cache
        else {
            Index::QueryResultVec query_results;
            {
                auto locked_tag_cache = tag_cache_ptr.Lock();
                locked_tag_cache->MakeQuery(query);
                locked_tag_cache->PollQueryResults(&query_results);
            }
            ProcessIndexQueryResults(query_results, &local_index_misses);
        }
        // Finally send to index tier if misses exist
        if(!local_index_misses.empty()){
            ProcessLocalIndexMisses(local_index_misses, logspace_id);
        }
    } else if (indexing_strategy_ == IndexingStrategy::COMPLETE){
        // complete index
        bool use_complete_index = true;
        if (absl::GetFlag(FLAGS_slog_engine_force_remote_index)) {
            use_complete_index = false;
        }
        if (absl::GetFlag(FLAGS_slog_engine_prob_remote_index) > 0.0f) {
            float coin = utils::GetRandomFloat(0.0f, 1.0f);
            if (coin < absl::GetFlag(FLAGS_slog_engine_prob_remote_index)) {
                use_complete_index = false;
            }
        }
        if (index_ptr != nullptr && use_complete_index) {
            IndexQuery query = BuildIndexQuery(op);
            Index::QueryResultVec query_results;
            {
                auto locked_index = index_ptr.Lock();
                locked_index->MakeQuery(query);
                locked_index->PollQueryResults(&query_results);
            }
            ProcessIndexQueryResultsComplete(query_results);
        } else {
            HVLOG_F(1, "There is no index for logspace {}. Send to index tier", logspace_id);
            HandleIndexTierRead(op, view_id, storage_shard);
        }
#ifdef __FAAS_OP_TRACING
    SaveTracePoint(op->id, "CompleteIndexQueryingDone");
#endif
    } else {
        UNREACHABLE();
    }
}

void Engine::HandleLocalSetAuxData(LocalOp* op) {
    uint64_t seqnum = op->seqnum;
    LogCachePutAuxData(seqnum, op->data.to_span());
    Message response = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::AUXDATA_OK, seqnum);
    FinishLocalOpWithResponse(op, &response, /* metalog_progress= */ 0);
    if (!absl::GetFlag(FLAGS_slog_engine_propagate_auxdata)) {
        return;
    }
    if (auto log_entry = LogCacheGet(seqnum); log_entry.has_value()) {
        if (auto aux_data = LogCacheGetAuxData(seqnum); aux_data.has_value()) {
            uint16_t view_id = log_utils::GetViewId(seqnum);
            absl::ReaderMutexLock view_lk(&view_mu_);
            if (view_id < views_.size()) {
                const View* view = views_.at(view_id);
                uint16_t sequencer_id = bits::LowHalf32(bits::HighHalf64(seqnum));
                uint16_t storage_shard_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(log_entry->metadata.localid));
                const View::StorageShard* storage_shard = view->GetStorageShard(bits::JoinTwo16(sequencer_id, storage_shard_id));
                PropagateAuxData(view, storage_shard, log_entry->metadata, *aux_data);
            }
        }
    }
}

#undef ONHOLD_IF_SEEN_FUTURE_VIEW

// Start handlers for remote messages

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
            HLOG(WARNING) << "Receive outdate request from view "   \
                          << (MESSAGE_VAR).view_id;                 \
            return;                                                 \
        }                                                           \
    } while (0)

void Engine::OnRecvNewMetaLogs(const SharedLogMessage& message,
                               std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), message.logspace_id);
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    Index::QueryResultVec local_index_misses;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        IGNORE_IF_NO_CONNECTION_FOR_LOGSPACE(message.logspace_id);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
#ifdef __FAAS_OP_TRACING
            for(auto const &[c, pending_append] : locked_producer->GetPendingAppends()){
                LocalOp* op = reinterpret_cast<LocalOp*>(pending_append);
                SaveOrIncreaseTracePoint(op->id, "ReceiveNewMetaLogs");
            }
#endif
            for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                locked_producer->ProvideMetaLog(metalog_proto);
            }
            locked_producer->PollAppendResults(&append_results);
        }
        if (indexing_strategy_ == IndexingStrategy::DISTRIBUTED){
            auto suffix_chain_ptr = suffix_chain_collection_.GetLogSpaceChecked(message.sequencer_id);
            {
                auto locked_suffix_chain = suffix_chain_ptr.Lock();
                for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                    locked_suffix_chain->ProvideMetaLog(metalog_proto);
                }
                locked_suffix_chain->PollQueryResults(&query_results);
            }
        } else if (indexing_strategy_ == IndexingStrategy::COMPLETE){
            auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
            {
                auto locked_index = index_ptr.Lock();
                for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                    locked_index->ProvideMetaLog(metalog_proto);
                }
                locked_index->PollQueryResults(&query_results);
            }
        }
    }
    ProcessAppendResults(append_results);
    if (indexing_strategy_ == IndexingStrategy::DISTRIBUTED){
        ProcessIndexQueryResults(query_results, &local_index_misses);
        if(!local_index_misses.empty()){
            ProcessLocalIndexMisses(local_index_misses, message.logspace_id);
        }
    } else if (indexing_strategy_ == IndexingStrategy::COMPLETE) {
        ProcessIndexQueryResultsComplete(query_results);
    }
}

void Engine::OnRecvNewIndexData(const SharedLogMessage& message,
                                std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::INDEX_DATA);
    HVLOG_F(1, "New index data from storage node {}", message.origin_node_id);
    IndexDataProto index_data_proto;
    if (!index_data_proto.ParseFromArray(payload.data(),
                                         static_cast<int>(payload.size()))) {
        LOG(FATAL) << "Failed to parse IndexDataProto";
    }
    Index::QueryResultVec query_results;
    Index::QueryResultVec local_index_misses;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_NO_CONNECTION_FOR_LOGSPACE(message.logspace_id);
        DCHECK(message.view_id < views_.size());
        if (indexing_strategy_ == IndexingStrategy::DISTRIBUTED) {
            // feed tag cache
            auto tag_cache_ptr = tag_cache_collection_.GetLogSpaceChecked(message.sequencer_id);
            {
                auto locked_tag_cache = tag_cache_ptr.Lock();
                locked_tag_cache->ProvideIndexData(
                    message.view_id,
                    index_data_proto
                );
                locked_tag_cache->PollQueryResults(&query_results);
            }
        } else if (indexing_strategy_ == IndexingStrategy::COMPLETE) {
            // feed complete index
            auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
            {
                auto locked_index = index_ptr.Lock();
                locked_index->ProvideIndexData(index_data_proto);
                locked_index->AdvanceIndexProgress();
                locked_index->PollQueryResults(&query_results);
            }
        }
    }
    if (indexing_strategy_ == IndexingStrategy::DISTRIBUTED) {
        ProcessIndexQueryResults(query_results, &local_index_misses);
        if(!local_index_misses.empty()){
            ProcessLocalIndexMisses(local_index_misses, message.logspace_id);
        }
    } else if (indexing_strategy_ == IndexingStrategy::COMPLETE) {
        ProcessIndexQueryResultsComplete(query_results);
    } 
}

void Engine::ProcessLocalIndexMisses(const Index::QueryResultVec& misses, uint32_t logspace_id){
    absl::ReaderMutexLock view_lk(&view_mu_);
    for(const IndexQueryResult& miss : misses){
        HandleIndexTierRead(
            onging_reads_.GetChecked(miss.original_query.client_data), 
            current_view_->id(), 
            view_mutable_.GetMyStorageShard(logspace_id)
        );
    }
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW
#undef IGNORE_IF_NO_CONNECTION_TO_LOGSPACE

void Engine::OnRecvResponse(const SharedLogMessage& message,
                            std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::RESPONSE);
    SharedLogResultType result = SharedLogMessageHelper::GetResultType(message);
    if (    result == SharedLogResultType::READ_OK
         || result == SharedLogResultType::EMPTY
         || result == SharedLogResultType::DATA_LOST) {
        uint64_t op_id = message.client_data;
        LocalOp* op;
        if (!onging_reads_.Poll(op_id, &op)) {
            HLOG_F(WARNING, "Cannot find read op with id {}. result_type={}, origin_node_id={}", 
                op_id, uint16_t(result), message.origin_node_id
            );
            return;
        }
#ifdef __FAAS_OP_TRACING
        SaveTracePoint(op->id, "ReceiveResponseFrom(Index|Storage)");
#endif
        if (result == SharedLogResultType::READ_OK) {
            uint64_t seqnum = bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf);
            HVLOG_F(1, "Receive remote read response for log (seqnum {})", seqnum);
            std::span<const uint64_t> user_tags;
            std::span<const char> log_data;
            std::span<const char> aux_data;
            log_utils::SplitPayloadForMessage(message, payload, &user_tags, &log_data, &aux_data);
            Message response = BuildLocalReadOKResponse(seqnum, user_tags, log_data);
            if (aux_data.size() > 0) {
                response.log_aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
                MessageHelper::AppendInlineData(&response, aux_data);
            }
            FinishLocalOpWithResponse(op, &response, message.user_metalog_progress);
            // Put the received log entry into log cache
            LogMetaData log_metadata = log_utils::GetMetaDataFromMessage(message);
            LogCachePut(log_metadata, user_tags, log_data);
            if (aux_data.size() > 0) {
                LogCachePutAuxData(seqnum, aux_data);
            }
        } else if (result == SharedLogResultType::EMPTY) {
            HLOG_F(INFO, "Receive EMPTY response for read request: seqnum={}, tag={}",
                   op->seqnum, op->query_tag);
            FinishLocalOpWithFailure(
                op, SharedLogResultType::EMPTY, message.user_metalog_progress);
        } else if (result == SharedLogResultType::DATA_LOST) {
            HLOG_F(WARNING, "Receive DATA_LOST response for read request: seqnum={}, tag={}",
                   op->seqnum, op->query_tag);
            FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
        } else {
            UNREACHABLE();
        }
    } else if (   result == SharedLogResultType::INDEX_OK
               || result == SharedLogResultType::INDEX_MIN_OK
               || result == SharedLogResultType::INDEX_MIN_FAILED) {
        IndexResultProto index_result_proto;
        if (!index_result_proto.ParseFromArray(payload.data(),
                                        static_cast<int>(payload.size()))) {
            LOG(FATAL) << "IndexRead: Failed to parse IndexFoundResultProto";
        }
        if (result == SharedLogResultType::INDEX_OK) {
            DCHECK(index_result_proto.found());
            // Put the received seqnum into seqnum cache (if not a tag based request) 
            if (message.query_tag == kEmptyLogTag && seqnum_cache_.has_value()){
                seqnum_cache_->Put(index_result_proto.seqnum(), gsl::narrow_cast<uint16_t>(index_result_proto.storage_shard_id()));
            }
        } else if (result == SharedLogResultType::INDEX_MIN_OK) {
            DCHECK(message.query_seqnum == 0);
            absl::ReaderMutexLock view_lk(&view_mu_);
            LockablePtr<TagCache> tag_cache_ptr;
            {
                tag_cache_ptr = tag_cache_collection_.GetLogSpaceChecked(message.sequencer_id);
            }
            {
                auto locked_tag_cache = tag_cache_ptr.Lock();
                locked_tag_cache->ProvideMinSeqnumData(message.user_logspace, message.query_tag, index_result_proto);
            }
        } else if (result == SharedLogResultType::INDEX_MIN_FAILED) {
            LOG_F(ERROR, "Seqnum min request at index node for tag {} failed", message.origin_node_id, message.query_tag);
        }
        else {
            UNREACHABLE();
        }
    }
    else {
        HLOG(FATAL) << "Unknown result type: " << message.op_result;
    }
}

void Engine::OnRecvRegistrationResponse(const protocol::SharedLogMessage& received_message){
    SharedLogResultType result = SharedLogMessageHelper::GetResultType(received_message);
    DCHECK_EQ(my_node_id(), received_message.engine_node_id);
    absl::MutexLock view_lk(&view_mu_);
    if(result == protocol::SharedLogResultType::REGISTER_STORAGE_FAILED){
        HLOG_F(ERROR, "Registration at storage_node={} failed. registration_view={}, response_view={}", 
            received_message.origin_node_id, current_view_->id(), received_message.view_id);
        return;
    }
    if(result == protocol::SharedLogResultType::REGISTER_INDEX_FAILED){
        HLOG_F(ERROR, "Registration at index_node={} failed. registration_view={}, response_view={}", 
            received_message.origin_node_id, current_view_->id(), received_message.view_id);
        return;
    }
    if(result == protocol::SharedLogResultType::REGISTER_SEQUENCER_FAILED){
        HLOG_F(ERROR, "Registration at sequencer_node={} failed. registration_view={}, response_view={}", 
            received_message.origin_node_id, current_view_->id(), received_message.view_id);
        return;
    }
    if(current_view_->id() != received_message.view_id){
        HLOG_F(WARNING, "View has changed. registration_view={}, response_view={}", current_view_->id(), received_message.view_id);
        return;
    }
    if (!current_view_->is_active_phylog(received_message.sequencer_id)) {
        HLOG_F(ERROR, "Sequencer_node={} is not active anymore", received_message.sequencer_id);
        return;
    }
    uint32_t logspace_id = bits::JoinTwo16(received_message.view_id, received_message.sequencer_id);
    if (result == protocol::SharedLogResultType::REGISTER_SEQUENCER_OK){
        DCHECK_EQ(received_message.origin_node_id, received_message.sequencer_id);
        HLOG_F(INFO, "Registered at sequencer_node={} and blocked shard_id={}", 
            received_message.origin_node_id, received_message.shard_id);
        if(view_mutable_.UpdateSequencerConnection(logspace_id, received_message.origin_node_id, received_message.local_start_id)){
            SharedLogMessage request = protocol::SharedLogMessageHelper::NewRegisterResponseMessage(SharedLogResultType::REGISTER_ENGINE,
                current_view_->id(), received_message.sequencer_id, received_message.shard_id, received_message.engine_node_id, received_message.local_start_id
            );
            const View::NodeIdVec storage_node_ids = current_view_->GetStorageNodes();
            for(uint16_t storage_node_id : storage_node_ids){
                HVLOG_F(1, "Send registration request to storage node. engine_node={}, storage_node={}, view_id={}, sequencer_id={}, storage_shard_id={}, local_start_id={}",
                    my_node_id(), storage_node_id, current_view_->id(), received_message.sequencer_id, received_message.shard_id, received_message.local_start_id);
                SendRegistrationRequest(storage_node_id, protocol::ConnType::ENGINE_TO_STORAGE, &request);
            }
            const View::NodeIdVec index_node_ids = current_view_->GetIndexNodes();
            for(uint16_t index_node_id : index_node_ids){
                HVLOG_F(1, "Send registration request to index node. engine_node={}, index_node_id={}, view_id={}, sequencer_id={}, storage_shard_id={}, local_start_id={}",
                    my_node_id(), index_node_id, current_view_->id(), received_message.sequencer_id, received_message.shard_id, received_message.local_start_id);
                SendRegistrationRequest(index_node_id, protocol::ConnType::ENGINE_TO_INDEX, &request);
            }
        }
    }
    else if(result == protocol::SharedLogResultType::REGISTER_STORAGE_OK){
        HLOG_F(INFO, "Registered at storage node. storage_node={}, sequencer_id={}, storage_shard_id={}, engine_id={}", 
            received_message.origin_node_id, received_message.sequencer_id, received_message.shard_id, received_message.engine_node_id);
        if(view_mutable_.UpdateStorageConnections(logspace_id, received_message.origin_node_id)){
            HLOG(INFO) << "Registered at all storage and index nodes. Unblock shard at sequencer node";
            SharedLogMessage request = protocol::SharedLogMessageHelper::NewRegisterResponseMessage(
                SharedLogResultType::REGISTER_UNBLOCK, current_view_->id(), received_message.sequencer_id, 
                received_message.shard_id, received_message.engine_node_id,
                /* local_start_id*/ view_mutable_.GetLocalStartOfConnection(logspace_id)
            );
            SendRegistrationRequest(received_message.sequencer_id, protocol::ConnType::ENGINE_TO_SEQUENCER, &request);
        }
    }
    else if (result == protocol::SharedLogResultType::REGISTER_INDEX_OK) {
        HLOG_F(INFO, "Registered at index node. index_node={}, sequencer_id={}, storage_shard_id={}, engine_id={}", 
            received_message.origin_node_id, received_message.sequencer_id, received_message.shard_id, received_message.engine_node_id);
        if(view_mutable_.UpdateIndexConnections(logspace_id, received_message.origin_node_id)){
            HLOG(INFO) << "Registered at all storage and index nodes. Unblock shard at sequencer node";
            SharedLogMessage request = protocol::SharedLogMessageHelper::NewRegisterResponseMessage(
                SharedLogResultType::REGISTER_UNBLOCK, current_view_->id(), received_message.sequencer_id, 
                received_message.shard_id, received_message.engine_node_id,
                /* local_start_id*/ view_mutable_.GetLocalStartOfConnection(logspace_id)
            );
            SendRegistrationRequest(received_message.sequencer_id, protocol::ConnType::ENGINE_TO_SEQUENCER, &request);
        }
    }
    else if (result == protocol::SharedLogResultType::REGISTER_UNBLOCK){
        // registration finished
        DCHECK_EQ(received_message.origin_node_id, received_message.sequencer_id);
        HLOG_F(INFO, "Registration finished for sequencer_node={}, unblocked shard_id={}, local_start_id={}", 
            received_message.origin_node_id, received_message.shard_id, received_message.local_start_id
        );
        const View::StorageShard* storage_shard = current_view_->GetStorageShard(bits::JoinTwo16(received_message.sequencer_id, received_message.shard_id));
        view_mutable_.UpdateMyStorageShards(logspace_id, storage_shard);
        uint32_t local_start_id = view_mutable_.GetLocalStartOfConnection(received_message.logspace_id);
        DCHECK_EQ(local_start_id, received_message.local_start_id);
        HVLOG_F(1, "Create logspace for view={}, sequencer={}", current_view_->id(), received_message.sequencer_id);
        producer_collection_.InstallLogSpace(std::make_unique<LogProducer>(received_message.shard_id, current_view_, received_message.sequencer_id, received_message.local_start_id));
        if (indexing_strategy_ == IndexingStrategy::DISTRIBUTED) {
            if(!suffix_chain_collection_.LogSpaceExists(received_message.sequencer_id)){
                suffix_chain_collection_.InstallLogSpace(std::make_unique<SeqnumSuffixChain>(received_message.sequencer_id, absl::GetFlag(FLAGS_slog_engine_seqnum_suffix_cap), 0.2));
            }
            auto suffix_chain_ptr = suffix_chain_collection_.GetLogSpaceChecked(received_message.sequencer_id);
            {
                auto locked_suffix_chain = suffix_chain_ptr.Lock();
                locked_suffix_chain->Extend(current_view_);
            }
            if(!tag_cache_collection_.LogSpaceExists(received_message.sequencer_id)){
                tag_cache_collection_.InstallLogSpace(std::make_unique<TagCache>(received_message.sequencer_id, absl::GetFlag(FLAGS_slog_engine_tag_cache_cap)));
            }
            auto tag_cache_ptr = tag_cache_collection_.GetLogSpaceChecked(received_message.sequencer_id);
            {
                auto locked_tag_cache_ptr = tag_cache_ptr.Lock();
                locked_tag_cache_ptr->InstallView(current_view_->id());
            }
            // seq cache is built in constructor and physical log independent
        } else if (indexing_strategy_ == IndexingStrategy::COMPLETE) {
            index_collection_.InstallLogSpace(std::make_unique<Index>(current_view_, received_message.sequencer_id));
        }
        // run ready requests
        std::vector<SharedLogRequest> ready_requests;
        future_requests_.OnNewView(current_view_, &ready_requests);
        current_view_active_ = true;
        HLOG(INFO) << "Current view active. Vamos!";
        if (!ready_requests.empty()) {
            HLOG_F(INFO, "{} requests for the new view", ready_requests.size());
            SomeIOWorker()->ScheduleFunction(
                nullptr, [this, requests = std::move(ready_requests)] () {
                    ProcessRequests(requests);
            });
        }
    }
    else {
        HLOG(ERROR) << "Registration logic unreachable";
        UNREACHABLE();
    }
}

void Engine::ProcessAppendResults(const LogProducer::AppendResultVec& results) {
    for (const LogProducer::AppendResult& result : results) {
        LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
#ifdef __FAAS_OP_TRACING
        SaveTracePoint(op->id, "ProcessAppendResult");
#endif
        if (result.seqnum != kInvalidLogSeqNum) {
            LogMetaData log_metadata = MetaDataFromAppendOp(op);
            log_metadata.seqnum = result.seqnum;
            log_metadata.localid = result.localid;
            LogCachePut(log_metadata, VECTOR_AS_SPAN(op->user_tags), op->data.to_span());
            Message response = MessageHelper::NewSharedLogOpSucceeded(
                SharedLogResultType::APPEND_OK, result.seqnum);
            FinishLocalOpWithResponse(op, &response, result.metalog_progress);
        } else {
            FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
        }
    }
}

void Engine::ProcessIndexFoundResult(const IndexQueryResult& query_result) {
    DCHECK(query_result.state == IndexQueryResult::kFound);
    const IndexQuery& query = query_result.original_query;
    bool local_request = (query.origin_node_id == my_node_id());
    uint64_t seqnum = query_result.found_result.seqnum;
    if (auto cached_log_entry = LogCacheGet(seqnum); cached_log_entry.has_value()) {
        // Cache hits
        HVLOG_F(1, "Cache hits for log entry (seqnum {})", bits::HexStr0x(seqnum));
#ifdef __FAAS_OP_STAT
        log_cache_hit_counter_.fetch_add(1, std::memory_order_acq_rel);
#endif 
        const LogEntry& log_entry = cached_log_entry.value();
        std::optional<std::string> cached_aux_data = LogCacheGetAuxData(seqnum);
        std::span<const char> aux_data;
        if (cached_aux_data.has_value()) {
            size_t full_size = log_entry.data.size()
                             + log_entry.user_tags.size() * sizeof(uint64_t)
                             + cached_aux_data->size();
            if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
                aux_data = STRING_AS_SPAN(*cached_aux_data);
            } else {
                HLOG_F(WARNING, "Inline buffer of message not large enough "
                                "for auxiliary data of log (seqnum {}): "
                                "log_size={}, num_tags={} aux_data_size={}",
                       bits::HexStr0x(seqnum), log_entry.data.size(),
                       log_entry.user_tags.size(), cached_aux_data->size());
            }
        }
        if (local_request) {
            LocalOp* op = onging_reads_.PollChecked(query.client_data);
#ifdef __FAAS_OP_TRACING
    SaveTracePoint(op->id, "ProcessIndexFoundResult");
#endif
            Message response = BuildLocalReadOKResponse(log_entry);
            response.log_aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
            MessageHelper::AppendInlineData(&response, aux_data);
            FinishLocalOpWithResponse(op, &response, query_result.metalog_progress);
        } else {
            HVLOG_F(1, "Send read response for log (seqnum {})", bits::HexStr0x(seqnum));
            SharedLogMessage response = SharedLogMessageHelper::NewReadOkResponse();
            log_utils::PopulateMetaDataToMessage(log_entry.metadata, &response);
            response.user_metalog_progress = query_result.metalog_progress;
            response.aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
            SendReadResponse(query, &response,
                             VECTOR_AS_CHAR_SPAN(log_entry.user_tags),
                             STRING_AS_SPAN(log_entry.data), aux_data);
        }
    } else {
        // Cache miss
#ifdef __FAAS_OP_STAT
        log_cache_miss_counter_.fetch_add(1, std::memory_order_acq_rel);
#endif 
        const View::StorageShard* storage_shard = nullptr;
        {
            absl::ReaderMutexLock view_lk(&view_mu_);
            uint16_t view_id = query_result.found_result.view_id;
            if (view_id < views_.size()) {
                const View* view = views_.at(view_id);
                storage_shard = view->GetStorageShard(query_result.StorageShardId());
            } else {
                HLOG_F(FATAL, "Cannot find view {}", view_id);
            }
        }
        bool success = SendStorageReadRequest(query_result, storage_shard);
        if (!success) {
            HLOG_F(WARNING, "Failed to send read request for seqnum {} ", bits::HexStr0x(seqnum));
            if (local_request) {
                LocalOp* op = onging_reads_.PollChecked(query.client_data);
                FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
            } else {
                SendReadFailureResponse(query, SharedLogResultType::DATA_LOST);
            }
        }
    }
}

void Engine::ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                        Index::QueryResultVec* more_results) {
    DCHECK(query_result.state == IndexQueryResult::kContinue);
    HVLOG_F(1, "Process IndexContinueResult: next_view_id={}",
            query_result.next_view_id);
    const IndexQuery& query = query_result.original_query;
    const View::Sequencer* sequencer_node = nullptr;
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        uint16_t view_id = query_result.next_view_id;
        if (view_id >= views_.size()) {
            HLOG_F(FATAL, "Cannot find view {}", view_id);
        }
        const View* view = views_.at(view_id);
        uint32_t logspace_id = view->LogSpaceIdentifier(query.user_logspace);
        sequencer_node = view->GetSequencerNode(bits::LowHalf32(logspace_id));
        index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
    }
    if (index_ptr != nullptr) {
        HVLOG(1) << "Use local index";
        IndexQuery query = BuildIndexQuery(query_result);
        auto locked_index = index_ptr.Lock();
        locked_index->MakeQuery(query);
        locked_index->PollQueryResults(more_results);
    } else {
        // TODO: sent to index tier
        HVLOG(1) << "Send to remote index";
        SharedLogMessage request = BuildReadRequestMessage(query_result);
        bool send_success = SendIndexReadRequest(DCHECK_NOTNULL(sequencer_node), &request);
        if (!send_success) {
            uint32_t logspace_id = bits::JoinTwo16(sequencer_node->view()->id(),
                                                   sequencer_node->node_id());
            HLOG_F(ERROR, "Failed to send read index request for logspace {}",
                   bits::HexStr0x(logspace_id));
        }
    }
}

void Engine::ProcessIndexQueryResults(const Index::QueryResultVec& results, Index::QueryResultVec* local_index_misses) {
    Index::QueryResultVec more_results;
    for (const IndexQueryResult& result : results) {
        const IndexQuery& query = result.original_query;
        switch (result.state) {
        case IndexQueryResult::kFound:
#ifdef __FAAS_OP_STAT
        local_index_hit_counter_.fetch_add(1, std::memory_order_acq_rel);
#endif 
            ProcessIndexFoundResult(result);
            if (result.original_query.user_tag == kEmptyLogTag){
                if (seqnum_cache_.has_value()){
                    seqnum_cache_->Put(result.found_result.seqnum, result.found_result.storage_shard_id);
                }
            }
            break;
        case IndexQueryResult::kEmpty:
            local_index_misses->push_back(result);
            break;
        case IndexQueryResult::kContinue:
            ProcessIndexContinueResult(result, &more_results);
            break;
        case IndexQueryResult::kInvalid:
            FinishLocalOpWithFailure(
                onging_reads_.PollChecked(query.client_data),
                SharedLogResultType::EMPTY, result.metalog_progress);
            break;
        default:
            UNREACHABLE();
        }
    }
    if (!more_results.empty()) {
        ProcessIndexQueryResults(more_results, local_index_misses);
    }
}

void Engine::ProcessIndexQueryResultsComplete(const Index::QueryResultVec& results) {
    Index::QueryResultVec more_results;
    for (const IndexQueryResult& result : results) {
        const IndexQuery& query = result.original_query;
        switch (result.state) {
        case IndexQueryResult::kFound:
            ProcessIndexFoundResult(result);
            break;
        case IndexQueryResult::kEmpty:
            FinishLocalOpWithFailure(onging_reads_.PollChecked(query.client_data),
                SharedLogResultType::EMPTY, result.metalog_progress);
            break;
        case IndexQueryResult::kContinue:
            ProcessIndexContinueResult(result, &more_results);
            break;
        default:
            UNREACHABLE();
        }
    }
    if (!more_results.empty()) {
        ProcessIndexQueryResultsComplete(more_results);
    }
}

void Engine::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        if (request.local_op == nullptr) {
            MessageHandler(request.message, STRING_AS_SPAN(request.payload));
        } else {
            LocalOpHandler(reinterpret_cast<LocalOp*>(request.local_op));
        }
    }
}

SharedLogMessage Engine::BuildReadRequestMessage(LocalOp* op) {
    DCHECK(  op->type == SharedLogOpType::READ_NEXT
          || op->type == SharedLogOpType::READ_PREV
          || op->type == SharedLogOpType::READ_NEXT_B);
    SharedLogMessage request = SharedLogMessageHelper::NewReadMessage(op->type);
    request.origin_node_id = my_node_id();
    request.hop_times = 1;
    request.client_data = op->id;
    request.user_logspace = op->user_logspace;
    request.query_tag = op->query_tag;
    request.query_seqnum = op->seqnum;
    request.user_metalog_progress = op->metalog_progress;
    request.flags |= protocol::kReadInitialFlag;
    request.prev_view_id = 0;
    request.prev_shard_id = 0;
    request.prev_found_seqnum = kInvalidLogSeqNum;
    return request;
}

SharedLogMessage Engine::BuildIndexTierReadRequestMessage(LocalOp* op, uint16_t master_node_id) {
    SharedLogMessage request = BuildReadRequestMessage(op);
    request.use_master_node_id = protocol::kUseMasterNodeId;
    request.master_node_id = master_node_id;
    return request;
}

SharedLogMessage Engine::BuildIndexTierMinSeqnumRequestMessage(LocalOp* op, uint64_t tag, uint64_t log_tail_seqnum) {
    SharedLogMessage request = SharedLogMessageHelper::NewReadMessage(SharedLogOpType::READ_MIN);
    request.origin_node_id = my_node_id();
    request.hop_times = 1;
    request.client_data = op->id;
    request.user_logspace = op->user_logspace;
    request.query_tag = tag;
    request.query_seqnum = 0;
    request.user_metalog_progress = op->metalog_progress;
    request.flags |= protocol::kReadInitialFlag;
    request.prev_view_id = 0;
    request.prev_shard_id = 0;
    request.tail_seqnum = log_tail_seqnum;
    return request;
}

SharedLogMessage Engine::BuildReadRequestMessage(const IndexQueryResult& result) {
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

IndexQuery Engine::BuildIndexQuery(LocalOp* op) {
    return IndexQuery {
        .direction = IndexQuery::DirectionFromOpType(op->type),
        .origin_node_id = my_node_id(),
        .hop_times = 0,
        .initial = true,
        .client_data = op->id,
        .user_logspace = op->user_logspace,
        .user_tag = op->query_tag,
        .query_seqnum = op->seqnum,
        .metalog_progress = op->metalog_progress,
        .prev_found_result = {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = kInvalidLogSeqNum
        }
    };
}

IndexQuery Engine::BuildIndexTierQuery(LocalOp* op, uint16_t master_node_id) {
    IndexQuery index_query = BuildIndexQuery(op);
    index_query.master_node_id = master_node_id;
    return index_query;
}

IndexQuery Engine::BuildIndexQuery(const SharedLogMessage& message) {
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
        .prev_found_result = IndexFoundResult {
            .view_id = message.prev_view_id,
            .storage_shard_id = message.prev_shard_id,
            .seqnum = message.prev_found_seqnum
        }
    };
}

IndexQuery Engine::BuildIndexQuery(const IndexQueryResult& result) {
    DCHECK(result.state == IndexQueryResult::kContinue);
    IndexQuery query = result.original_query;
    query.initial = false;
    query.metalog_progress = result.metalog_progress;
    query.prev_found_result = result.found_result;
    return query;
}

#ifdef __FAAS_STAT_THREAD
    void Engine::StatisticsThreadMain() {
        int timerfd = io_utils::CreateTimerFd();
        CHECK(timerfd != -1) << "Failed to create timerfd";
        io_utils::FdUnsetNonblocking(timerfd);
        absl::Duration interval = absl::Seconds(absl::GetFlag(FLAGS_slog_engine_stat_thread_interval));
        CHECK(io_utils::SetupTimerFdPeriodic(timerfd, interval, interval))
            << "Failed to setup timerfd with interval " << interval;
#ifdef __FAAS_OP_LATENCY
        ResetOpLatencies();
#endif
#ifdef __FAAS_OP_STAT
        ResetOpStat();
#endif
        bool running = true;
        while (running) {
            uint64_t exp;
            ssize_t nread = read(timerfd, &exp, sizeof(uint64_t));
            if (nread < 0) {
                PLOG(FATAL) << "Failed to read on timerfd";
            }
            CHECK_EQ(gsl::narrow_cast<size_t>(nread), sizeof(uint64_t));
            uint64_t local_op_id = LoadLocalOpId();
            if (previous_local_op_id_ != local_op_id){
                int64_t now_ts = GetRealtimeSecondTimestamp();
#ifdef __FAAS_OP_LATENCY
                std::ostringstream append_latencies;
                std::ostringstream read_latencies;
                PrintOpLatencies(&append_latencies, &read_latencies);
                {
                    std::ofstream latency_file;
                    latency_file.open(fmt::format("/tmp/slog/stats/latencies-append-{}-all.csv", my_node_id()), std::fstream::app);
                    latency_file << append_latencies.str();
                    latency_file.close();
                }
                {
                    std::ofstream latency_file;
                    latency_file.open(fmt::format("/tmp/slog/stats/latencies-read-{}-all.csv", my_node_id()), std::fstream::app);
                    latency_file << read_latencies.str();
                    latency_file.close();
                }
                {
                    std::ofstream latency_file;
                    latency_file.open(fmt::format("/tmp/slog/stats/latencies-append-{}-{}.csv", my_node_id(), now_ts), std::fstream::app);
                    latency_file << append_latencies.str();
                    latency_file.close();
                }
                {
                    std::ofstream latency_file;
                    latency_file.open(fmt::format("/tmp/slog/stats/latencies-read-{}-{}.csv", my_node_id(), now_ts), std::fstream::app);
                    latency_file << read_latencies.str();
                    latency_file.close();
                }
#endif
#ifdef __FAAS_OP_STAT
                std::ofstream op_st_file(fmt::format("/tmp/slog/stats/op-stat-{}-{}", my_node_id(), now_ts));
                op_st_file
                    << std::to_string(append_ops_counter_.load())           << ","
                    << std::to_string(read_ops_counter_.load())             << "," 
                    << std::to_string(local_index_hit_counter_.load())      << "," 
                    << std::to_string(local_index_miss_counter_.load())     << "," 
                    << std::to_string(index_min_read_ops_counter_.load())   << "," 
                    << std::to_string(log_cache_hit_counter_.load())        << "," 
                    << std::to_string(log_cache_miss_counter_.load())       << "\n"  
                ;
                op_st_file.close();
#endif
#ifdef __FAAS_OP_TRACING
                absl::MutexLock trace_mu_lk(&trace_mu_);
                std::ostringstream append_results;
                std::ostringstream read_results;
                auto it = finished_traces_.begin();
                while (it != finished_traces_.end()){
                    if (!traces_.contains(*it)){
                        PLOG(FATAL) << "Trace must still be in trace map";
                    }
                    PrintTrace(&append_results, &read_results, traces_.at(*it).get());
                    traces_.erase(*it);
                    ++it;
                }
                finished_traces_.clear();
                {
                    std::ofstream trace_file;
                    trace_file.open(fmt::format("/tmp/slog/stats/traces-append-{}", my_node_id()), std::fstream::app);
                    trace_file << append_results.str();
                    trace_file.close();
                }
                {
                    std::ofstream trace_file;
                    trace_file.open(fmt::format("/tmp/slog/stats/traces-read-{}", my_node_id()), std::fstream::app);
                    trace_file << read_results.str();
                    trace_file.close();
                }
#endif
#ifdef __FAAS_INDEX_MEMORY
                size_t suffix_chain_links = 0;
                size_t suffix_chain_num_ranges = 0;
                size_t suffix_chain_size = 0;
                size_t seqnum_cache_num_seqnums = 0;
                size_t seqnum_cache_size = 0;
                size_t tag_cache_num_tags = 0;
                size_t tag_cache_seqnums = 0;
                size_t tag_cache_size = 0;
                size_t complete_index_num_seqnums = 0;
                size_t complete_index_num_tags = 0;
                size_t complete_index_num_seqnums_of_tags = 0;
                size_t complete_index_size = 0;
                {
                    absl::ReaderMutexLock view_lk(&view_mu_);
                    if (indexing_strategy_ == IndexingStrategy::DISTRIBUTED){
                        suffix_chain_collection_.ForEachLogSpace(
                            [&] (uint32_t id, LockablePtr<SeqnumSuffixChain> suffix_chain_ptr) {
                                auto ptr = suffix_chain_ptr.Lock();
                                ptr->Aggregate(&suffix_chain_links, &suffix_chain_num_ranges, &suffix_chain_size);
                            }
                        );
                        if(seqnum_cache_.has_value()){
                            seqnum_cache_->Aggregate(&seqnum_cache_num_seqnums, &seqnum_cache_size);
                        }
                        tag_cache_collection_.ForEachLogSpace(
                            [&] (uint32_t id, LockablePtr<TagCache> tag_cache_ptr) {
                                auto ptr = tag_cache_ptr.Lock();
                                ptr->Aggregate(&tag_cache_num_tags, &tag_cache_seqnums, &tag_cache_size);
                            }
                        );
                    }
                    else if (indexing_strategy_ == IndexingStrategy::COMPLETE) {
                        index_collection_.ForEachActiveLogSpace(
                            [&] (uint32_t logspace_id, LockablePtr<Index> index_ptr) {
                                auto ptr = index_ptr.Lock();
                                ptr->Aggregate(&complete_index_num_seqnums, &complete_index_num_tags, &complete_index_num_seqnums_of_tags, &complete_index_size);
                            }
                        );
                        index_collection_.ForEachFinalizedLogSpace(
                            [&] (uint32_t logspace_id, LockablePtr<Index> index_ptr) {
                                auto ptr = index_ptr.Lock();
                                ptr->Aggregate(&complete_index_num_seqnums, &complete_index_num_tags, &complete_index_num_seqnums_of_tags, &complete_index_size);
                            }
                        );
                    }
                }
                std::ofstream ix_memory_file(fmt::format("/tmp/slog/stats/index-memory-{}-{}.csv", my_node_id(), now_ts));
                ix_memory_file
                    << std::to_string(suffix_chain_size + seqnum_cache_size + tag_cache_size + complete_index_size) << ","
                    << std::to_string(complete_index_size) << "," 
                    << std::to_string(complete_index_num_seqnums) << "," 
                    << std::to_string(complete_index_num_tags) << "," 
                    << std::to_string(suffix_chain_size) << ","
                    << std::to_string(suffix_chain_links) << ","
                    << std::to_string(suffix_chain_num_ranges) << ","
                    << std::to_string(seqnum_cache_size) << ","
                    << std::to_string(seqnum_cache_num_seqnums) << ","
                    << std::to_string(tag_cache_size) << ","
                    << std::to_string(tag_cache_num_tags) << ","
                    << std::to_string(tag_cache_seqnums) << ",";
                ix_memory_file.close();
#endif
                previous_local_op_id_ = local_op_id;
            }
        }
    }      
#endif

}  // namespace log
}  // namespace faas
