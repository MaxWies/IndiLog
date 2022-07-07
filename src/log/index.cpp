#include "log/index.h"

#include "log/utils.h"

namespace faas {
namespace log {

Index::Index(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      indexed_metalog_position_(0),
      data_received_seqnum_position_(0),
      indexed_seqnum_position_(0) {
    log_header_ = fmt::format("LogIndex[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

Index::~Index() {}

PerSpaceIndex::PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace)
    : logspace_id_(logspace_id),
      user_logspace_(user_logspace) {}

void PerSpaceIndex::Add(uint32_t seqnum_lowhalf, uint16_t engine_id,
                               const UserTagVec& user_tags) {
    DCHECK(!engine_ids_.contains(seqnum_lowhalf));
    engine_ids_[seqnum_lowhalf] = engine_id;
    DCHECK(seqnums_.empty() || seqnum_lowhalf > seqnums_.back());
    seqnums_.push_back(seqnum_lowhalf);
    for (uint64_t user_tag : user_tags) {
        DCHECK_NE(user_tag, kEmptyLogTag);
        seqnums_by_tag_[user_tag].push_back(seqnum_lowhalf);
    }
}

void PerSpaceIndex::Aggregate(size_t* num_seqnums, size_t* num_tags, size_t* num_seqnums_of_tags, size_t* size){
    *num_seqnums += seqnums_.size();
    *num_tags += seqnums_by_tag_.size();
    size_t local_num_seqnums_of_tags = 0;
    for (auto& [tag, seqnums] : seqnums_by_tag_){
        local_num_seqnums_of_tags += seqnums.size();
    }
    *num_seqnums_of_tags += local_num_seqnums_of_tags;
    *size += (
          sizeof(uint32_t) * engine_ids_.size()         // seqnum keys for engine_ids
        + sizeof(uint16_t) * engine_ids_.size()         // engine_ids
        + sizeof(uint32_t) * seqnums_.size()            // seqnums
        + sizeof(uint64_t) * seqnums_by_tag_.size()     // tags
        + sizeof(uint32_t) * local_num_seqnums_of_tags  // seqnums of tags
    );
}

bool PerSpaceIndex::FindPrev(uint64_t query_seqnum, uint64_t user_tag,
                                    uint64_t* seqnum, uint16_t* engine_id) const {
    uint32_t seqnum_lowhalf;
    if (user_tag == kEmptyLogTag) {
        if (!FindPrev(seqnums_, query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    } else {
        if (!seqnums_by_tag_.contains(user_tag)) {
            return false;
        }
        if (!FindPrev(seqnums_by_tag_.at(user_tag), query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    }
    DCHECK(engine_ids_.contains(seqnum_lowhalf));
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_LE(*seqnum, query_seqnum);
    *engine_id = engine_ids_.at(seqnum_lowhalf);
    return true;
}

bool PerSpaceIndex::FindNext(uint64_t query_seqnum, uint64_t user_tag,
                                    uint64_t* seqnum, uint16_t* engine_id) const {
    uint32_t seqnum_lowhalf;
    if (user_tag == kEmptyLogTag) {
        if (!FindNext(seqnums_, query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    } else {
        if (!seqnums_by_tag_.contains(user_tag)) {
            return false;
        }
        if (!FindNext(seqnums_by_tag_.at(user_tag), query_seqnum, &seqnum_lowhalf)) {
            return false;
        }
    }
    DCHECK(engine_ids_.contains(seqnum_lowhalf));
    *seqnum = bits::JoinTwo32(logspace_id_, seqnum_lowhalf);
    DCHECK_GE(*seqnum, query_seqnum);
    *engine_id = engine_ids_.at(seqnum_lowhalf);
    return true;
}

bool PerSpaceIndex::FindPrev(const std::vector<uint32_t>& seqnums,
                                    uint64_t query_seqnum, uint32_t* result_seqnum) const {
    if (seqnums.empty() || bits::JoinTwo32(logspace_id_, seqnums.front()) > query_seqnum) {
        return false;
    }
    if (query_seqnum == kMaxLogSeqNum) {
        *result_seqnum = seqnums.back();
        return true;
    }
    auto iter = absl::c_upper_bound(
        seqnums, query_seqnum,
        [logspace_id = logspace_id_] (uint64_t lhs, uint32_t rhs) {
            return lhs < bits::JoinTwo32(logspace_id, rhs);
        }
    );
    if (iter == seqnums.begin()) {
        return false;
    } else {
        *result_seqnum = *(--iter);
        return true;
    }
}

bool PerSpaceIndex::FindNext(const std::vector<uint32_t>& seqnums,
                                    uint64_t query_seqnum, uint32_t* result_seqnum) const {
    if (seqnums.empty() || bits::JoinTwo32(logspace_id_, seqnums.back()) < query_seqnum) {
        return false;
    }
    auto iter = absl::c_lower_bound(
        seqnums, query_seqnum,
        [logspace_id = logspace_id_] (uint32_t lhs, uint64_t rhs) {
            return bits::JoinTwo32(logspace_id, lhs) < rhs;
        }
    );
    if (iter == seqnums.end()) {
        return false;
    } else {
        *result_seqnum = *iter;
        return true;
    }
}

void Index::MakeQuery(const IndexQuery& query) {
    if (query.initial) {
        HVLOG(1) << "IndexRead: Receive initial query";
        uint16_t view_id = log_utils::GetViewId(query.metalog_progress);
        if (view_id > view_->id()) {
            HLOG_F(FATAL, "IndexRead: Cannot process query with metalog_progress from the future: "
                          "metalog_progress={}, my_view_id={}",
                   bits::HexStr0x(query.metalog_progress), bits::HexStr0x(view_->id()));
        } else if (view_id < view_->id()) {
            HVLOG(1) << "IndexRead: Can process query. View in index higher.";
            ProcessQuery(query);
        } else {
            DCHECK_EQ(view_id, view_->id());
            uint32_t position = bits::LowHalf64(query.metalog_progress);
            if (position <= indexed_metalog_position_) {
                HVLOG(1) << "IndexRead: Can process query. Metalog in index equal or higher.";
                ProcessQuery(query);
            } else {
                HVLOG_F(1, "IndexRead: Query has higher metalog position: Query metalog_progress={}, Index metalog_progress={}. Add query to pending queries.", position, indexed_metalog_position_);
                pending_queries_.insert(std::make_pair(position, query));
            }
        }
    } else {
        HVLOG(1) << "IndexRead: Receive continue query";
        if (finalized()) {
            ProcessQuery(query);
        } else {
            pending_queries_.insert(std::make_pair(kMaxMetalogPosition, query));
        }
    }
}

void Index::PollQueryResults(IndexQueryResultVec* results) {
    if (pending_query_results_.empty()) {
        return;
    }
    if (results->empty()) {
        *results = std::move(pending_query_results_);
    } else {
        results->insert(results->end(),
                        pending_query_results_.begin(),
                        pending_query_results_.end());
    }
    pending_query_results_.clear();
}

void Index::OnMetaLogApplied(const MetaLogProto& meta_log_proto) {
    if (meta_log_proto.type() == MetaLogProto::NEW_LOGS) {
        const auto& new_logs_proto = meta_log_proto.new_logs_proto();
        uint32_t seqnum = new_logs_proto.start_seqnum();
        for (uint32_t delta : new_logs_proto.shard_deltas()) {
            seqnum += delta;
        }
        cuts_.push_back(std::make_pair(meta_log_proto.metalog_seqnum(), seqnum));
    }
}

void Index::OnFinalized(uint32_t metalog_position) {
    auto iter = pending_queries_.begin();
    while (iter != pending_queries_.end()) {
        DCHECK_EQ(iter->first, kMaxMetalogPosition);
        const IndexQuery& query = iter->second;
        ProcessQuery(query);
        iter = pending_queries_.erase(iter);
    }
}

PerSpaceIndex* Index::GetOrCreateIndex(uint32_t user_logspace) {
    if (index_.contains(user_logspace)) {
        return index_.at(user_logspace).get();
    }
    HVLOG_F(1, "IndexUpdate: Create index of user logspace {}", user_logspace);
    PerSpaceIndex* index = new PerSpaceIndex(identifier(), user_logspace);
    index_[user_logspace].reset(index);
    return index;
}

void Index::TryCreateIndex(uint32_t user_logspace) {
    if (!index_.contains(user_logspace)) {
        HVLOG_F(1, "IndexUpdate: Create index of user logspace {}", user_logspace);
        PerSpaceIndex* index = new PerSpaceIndex(identifier(), user_logspace);
        index_[user_logspace].reset(index);
    }
}

void Index::Aggregate(size_t* num_seqnums, size_t* num_tags, size_t* num_seqnums_of_tags, size_t* size) {
    for (auto& [user_logspace, index] : index_){
        index->Aggregate(num_seqnums, num_tags, num_seqnums_of_tags, size);
        *size += (
            sizeof(uint32_t) * 1 // key
        );
    }
}

void Index::ProcessQuery(const IndexQuery& query) {
    if (query.direction == IndexQuery::kReadNextB) {
        bool success = ProcessBlockingQuery(query);
        if (!success) {
            blocking_reads_.push_back(std::make_pair(GetMonotonicMicroTimestamp(), query));
        }
    } else if (query.direction == IndexQuery::kReadNext) {
        ProcessReadNext(query);
    } else if (query.direction == IndexQuery::kReadPrev) {
        ProcessReadPrev(query);
    }
}

void Index::ProcessReadNext(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNext);
    HVLOG_F(1, "IndexRead: ProcessReadNext: query_seqnum={}, logspace={}, tag={}",
            bits::HexStr0x(query.query_seqnum), query.user_logspace, query.user_tag);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id > view_->id()) {
        pending_query_results_.push_back(BuildEmptyResult(query));
        HVLOG(1) << "IndexRead: ProcessReadNext: EmptyResult";
        return;
    }
    uint64_t seqnum;
    uint16_t engine_id;
    bool found = IndexFindNext(query, &seqnum, &engine_id);
    if (query_view_id == view_->id()) {
        if (found) {
            pending_query_results_.push_back(
                BuildFoundResult(query, view_->id(), seqnum, engine_id));
            HVLOG_F(1, "IndexRead: ProcessReadNext: FoundResult: query_seqnum={}, seqnum={}", bits::HexStr0x(query.query_seqnum), bits::HexStr0x(seqnum));
        } else {
            if (query.prev_found_result.seqnum != kInvalidLogSeqNum) {
                const IndexFoundResult& found_result = query.prev_found_result;
                pending_query_results_.push_back(
                    BuildFoundResult(query, found_result.view_id,
                                     found_result.seqnum, found_result.storage_shard_id));
                HVLOG_F(1, "IndexRead: ProcessReadNext: FoundResult (from prev_result): query_seqnum={}, seqnum={}", bits::HexStr0x(query.query_seqnum), bits::HexStr0x(found_result.seqnum));
            } else {
                pending_query_results_.push_back(BuildEmptyResult(query));
                HVLOG(1) << "IndexRead: ProcessReadNext: EmptyResult";
            }
        }
    } else {
        pending_query_results_.push_back(
            BuildContinueResult(query, found, seqnum, engine_id));
        HVLOG(1) << "IndexRead: ProcessReadNext: ContinueResult";
    }
}

void Index::ProcessReadPrev(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadPrev);
    HVLOG_F(1, "IndexRead: ProcessReadPrev: query_seqnum={}, logspace={}, tag={}",
            bits::HexStr0x(query.query_seqnum), query.user_logspace, query.user_tag);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id < view_->id()) {
        pending_query_results_.push_back(BuildContinueResult(query, false, 0, 0));
        HVLOG(1) << "IndexRead: ProcessReadPrev: ContinueResult";
        return;
    }
    uint64_t seqnum;
    uint16_t engine_id;
    bool found = IndexFindPrev(query, &seqnum, &engine_id);
    if (found) {
        pending_query_results_.push_back(
            BuildFoundResult(query, view_->id(), seqnum, engine_id));
        HVLOG_F(1, "IndexRead: ProcessReadPrev: FoundResult: query_seqnum={}, seqnum={}", bits::HexStr0x(query.query_seqnum), bits::HexStr0x(seqnum));
    } else if (view_->id() > 0) {
        pending_query_results_.push_back(BuildContinueResult(query, false, 0, 0));
        HVLOG(1) << "IndexRead: ProcessReadPrev: ContinueResult";
    } else {
        pending_query_results_.push_back(BuildEmptyResult(query));
        HVLOG(1) << "IndexRead: ProcessReadPrev: EmptyResult";
    }
}

bool Index::ProcessBlockingQuery(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNextB && query.initial);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id > view_->id()) {
        pending_query_results_.push_back(BuildEmptyResult(query));
        return true;
    }
    uint64_t seqnum;
    uint16_t engine_id;
    bool found = IndexFindNext(query, &seqnum, &engine_id);
    if (query_view_id == view_->id()) {
        if (found) {
            pending_query_results_.push_back(
                BuildFoundResult(query, view_->id(), seqnum, engine_id));
        }
        return found;
    } else {
        pending_query_results_.push_back(
            BuildContinueResult(query, found, seqnum, engine_id));
        return true;
    }
}

bool Index::IndexFindNext(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id) {
    DCHECK(query.direction == IndexQuery::kReadNext
            || query.direction == IndexQuery::kReadNextB);
    if (!index_.contains(query.user_logspace)) {
        return false;
    }
    return GetOrCreateIndex(query.user_logspace)->FindNext(
        query.query_seqnum, query.user_tag, seqnum, engine_id);
}

bool Index::IndexFindPrev(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id) {
    DCHECK(query.direction == IndexQuery::kReadPrev);
    if (!index_.contains(query.user_logspace)) {
        return false;
    }
    return GetOrCreateIndex(query.user_logspace)->FindPrev(
        query.query_seqnum, query.user_tag, seqnum, engine_id);
}

IndexQueryResult Index::BuildFoundResult(const IndexQuery& query, uint16_t view_id,
                                         uint64_t seqnum, uint16_t storage_shard_id) {
    return IndexQueryResult {
        .state = IndexQueryResult::kFound,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = view_id,
            .storage_shard_id = storage_shard_id,
            .seqnum = seqnum
        }
    };
}

IndexQueryResult Index::BuildEmptyResult(const IndexQuery& query) {
    return IndexQueryResult {
        .state = IndexQueryResult::kEmpty,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = kInvalidLogSeqNum
        }
    };
}

IndexQueryResult Index::BuildContinueResult(const IndexQuery& query, bool found,
                                            uint64_t seqnum, uint16_t storage_shard_id) {
    DCHECK(view_->id() > 0);
    IndexQueryResult result = {
        .state = IndexQueryResult::kContinue,
        .metalog_progress = query.initial ? index_metalog_progress()
                                          : query.metalog_progress,
        .next_view_id = gsl::narrow_cast<uint16_t>(view_->id() - 1),
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = kInvalidLogSeqNum
        }
    };
    if (query.direction == IndexQuery::kReadNextB) {
        result.original_query.direction = IndexQuery::kReadNext;
    }
    if (!query.initial) {
        result.found_result = query.prev_found_result;
    }
    if (found) {
        result.found_result = IndexFoundResult {
            .view_id = view_->id(),
            .storage_shard_id = storage_shard_id,
            .seqnum = seqnum
        };
    } else if (!query.initial && query.prev_found_result.seqnum != kInvalidLogSeqNum) {
        result.found_result = query.prev_found_result;
    }
    return result;
}

}  // namespace log
}  // namespace faas
