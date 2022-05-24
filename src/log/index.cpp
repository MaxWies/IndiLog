#include "log/index.h"

#include "log/utils.h"

namespace faas {
namespace log {

IndexQuery::ReadDirection IndexQuery::DirectionFromOpType(protocol::SharedLogOpType op_type) {
    switch (op_type) {
    case protocol::SharedLogOpType::READ_NEXT:
    case protocol::SharedLogOpType::READ_NEXT_INDEX_RESULT:
        return IndexQuery::kReadNext;
    case protocol::SharedLogOpType::READ_PREV:
    case protocol::SharedLogOpType::READ_PREV_INDEX_RESULT:
        return IndexQuery::kReadPrev;
    case protocol::SharedLogOpType::READ_NEXT_B:
    case protocol::SharedLogOpType::READ_NEXT_B_INDEX_RESULT:
        return IndexQuery::kReadNextB;
    case protocol::SharedLogOpType::READ_MIN:
        return IndexQuery::kReadNext;
    default:
        UNREACHABLE();
    }
}

protocol::SharedLogOpType IndexQuery::DirectionToOpType() const {
    switch (direction) {
    case IndexQuery::kReadNext:
        return protocol::SharedLogOpType::READ_NEXT;
    case IndexQuery::kReadPrev:
        return protocol::SharedLogOpType::READ_PREV;
    case IndexQuery::kReadNextB:
        return protocol::SharedLogOpType::READ_NEXT_B;
    default:
        UNREACHABLE();
    }
}

protocol::SharedLogOpType IndexQuery::DirectionToIndexResult() const {
    switch (direction) {
    case IndexQuery::kReadNext:
        return protocol::SharedLogOpType::READ_NEXT_INDEX_RESULT;
    case IndexQuery::kReadPrev:
        return protocol::SharedLogOpType::READ_PREV_INDEX_RESULT;
    case IndexQuery::kReadNextB:
        return protocol::SharedLogOpType::READ_NEXT_B_INDEX_RESULT;
    default:
        UNREACHABLE();
    }
}

std::string IndexQuery::DirectionToString() const {
    switch (direction) {
    case IndexQuery::kReadNext:
        return "next";
    case IndexQuery::kReadPrev:
        return "prev";
    case IndexQuery::kReadNextB:
        return "nextB";
    default:
        UNREACHABLE();
    }
}

uint32_t IndexQueryResult::StorageShardId() const {
    CHECK_EQ(state, State::kFound);
    return bits::JoinTwo16(bits::LowHalf32(bits::HighHalf64(found_result.seqnum)), found_result.storage_shard_id);
}

bool IndexQueryResult::IsFound() const {
    return state == State::kFound;
}

bool IndexQueryResult::IsPointHit() const {
    if (state != State::kFound){
        return false;
    }
    return original_query.query_seqnum == found_result.seqnum;
}

Index::Index(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      indexed_metalog_position_(0),
      first_index_data_(true),
      data_received_seqnum_position_(0),
      indexed_seqnum_position_(0) {
    log_header_ = fmt::format("LogIndex[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

Index::~Index() {}

class Index::PerSpaceIndex {
public:
    PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace);
    ~PerSpaceIndex() {}

    void Add(uint32_t seqnum_lowhalf, uint16_t engine_id, const UserTagVec& user_tags);

    bool FindPrev(uint64_t query_seqnum, uint64_t user_tag,
                  uint64_t* seqnum, uint16_t* engine_id) const;
    bool FindNext(uint64_t query_seqnum, uint64_t user_tag,
                  uint64_t* seqnum, uint16_t* engine_id) const;

    void Aggregate(size_t* num_seqnums, size_t* num_tags, size_t* num_seqnums_of_tags, size_t* size);

private:
    uint32_t logspace_id_;
    uint32_t user_logspace_;

    absl::flat_hash_map</* seqnum */ uint32_t, uint16_t> engine_ids_;
    std::vector<uint32_t> seqnums_;
    absl::flat_hash_map</* tag */ uint64_t, std::vector<uint32_t>> seqnums_by_tag_;

    bool FindPrev(const std::vector<uint32_t>& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum) const;
    bool FindNext(const std::vector<uint32_t>& seqnums, uint64_t query_seqnum,
                  uint32_t* result_seqnum) const;

    DISALLOW_COPY_AND_ASSIGN(PerSpaceIndex);
};

Index::PerSpaceIndex::PerSpaceIndex(uint32_t logspace_id, uint32_t user_logspace)
    : logspace_id_(logspace_id),
      user_logspace_(user_logspace) {}

void Index::PerSpaceIndex::Add(uint32_t seqnum_lowhalf, uint16_t engine_id,
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

void Index::PerSpaceIndex::Aggregate(size_t* num_seqnums, size_t* num_tags, size_t* num_seqnums_of_tags, size_t* size){
    *num_seqnums += seqnums_.size();
    *num_tags += seqnums_by_tag_.size();
    size_t local_num_seqnums_of_tags = 0;
    for (auto& [tag, seqnums] : seqnums_by_tag_){
        local_num_seqnums_of_tags += seqnums.size();
    }
    *num_seqnums_of_tags += local_num_seqnums_of_tags;
    *size += (
        sizeof(uint32_t) * engine_ids_.size()           // seqnum keys for engine_ids
        + sizeof(uint16_t) * engine_ids_.size()         // engine_ids
        + sizeof(uint32_t) * seqnums_.size()            // seqnums
        + sizeof(uint64_t) * seqnums_by_tag_.size()     // tags
        + sizeof(uint32_t) * local_num_seqnums_of_tags  // seqnums of tags
    );
}

bool Index::PerSpaceIndex::FindPrev(uint64_t query_seqnum, uint64_t user_tag,
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

bool Index::PerSpaceIndex::FindNext(uint64_t query_seqnum, uint64_t user_tag,
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

bool Index::PerSpaceIndex::FindPrev(const std::vector<uint32_t>& seqnums,
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

bool Index::PerSpaceIndex::FindNext(const std::vector<uint32_t>& seqnums,
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

void Index::ProvideIndexData(const IndexDataProto& index_data) {
    DCHECK_EQ(identifier(), index_data.logspace_id());
    if(first_index_data_ && 0 < index_data.seqnum_halves_size()){
        HVLOG_F(1, "Initialize data_received_seqnum_position with {}", index_data.seqnum_halves().at(0));
        data_received_seqnum_position_ = index_data.seqnum_halves().at(0);
        first_index_data_ = false;
    }
    int n = index_data.seqnum_halves_size();
    DCHECK_EQ(n, index_data.engine_ids_size());
    DCHECK_EQ(n, index_data.user_logspaces_size());
    DCHECK_EQ(n, index_data.user_tag_sizes_size());
    uint32_t total_tags = absl::c_accumulate(index_data.user_tag_sizes(), 0U);
    DCHECK_EQ(static_cast<int>(total_tags), index_data.user_tags_size());
    auto tag_iter = index_data.user_tags().begin();
    for (int i = 0; i < n; i++) {
        size_t num_tags = index_data.user_tag_sizes(i);
        uint32_t seqnum = index_data.seqnum_halves(i);
        if (seqnum < indexed_seqnum_position_) {
            HVLOG_F(1, "Seqnum={} lower than IndexedSeqnumPosition={}", seqnum, indexed_seqnum_position_);
            tag_iter += num_tags;
            continue;
        }
        if (received_data_.count(seqnum) == 0) {
            received_data_[seqnum] = IndexData {
                .engine_id     = gsl::narrow_cast<uint16_t>(index_data.engine_ids(i)),
                .user_logspace = index_data.user_logspaces(i),
                .user_tags     = UserTagVec(tag_iter, tag_iter + num_tags)
            };
        } else {
#if DCHECK_IS_ON()
            const IndexData& data = received_data_[seqnum];
            DCHECK_EQ(data.engine_id,
                      gsl::narrow_cast<uint16_t>(index_data.engine_ids(i)));
            DCHECK_EQ(data.user_logspace, index_data.user_logspaces(i));
            DCHECK_EQ(data.user_tags.size(),
                      gsl::narrow_cast<size_t>(index_data.user_tag_sizes(i)));
#endif
        }
        tag_iter += num_tags;
    }
    while (received_data_.count(data_received_seqnum_position_) > 0) {
        data_received_seqnum_position_++;
    }
}

void Index::ProvideIndexData(const IndexDataProto& index_data, uint16_t my_index_node_id) {
    DCHECK_EQ(identifier(), index_data.logspace_id());
    if(first_index_data_){
        first_index_data_ = false;
    }
    uint16_t selected_index_shard = gsl::narrow_cast<uint16_t>(index_data.index_shard());
    bool is_my_turn = view_->GetIndexNode(my_index_node_id)->IsIndexShardMember(selected_index_shard);
    absl::flat_hash_set<uint64_t> min_tags_;
    int n = index_data.seqnum_halves_size();
    DCHECK_EQ(n, index_data.engine_ids_size());
    DCHECK_EQ(n, index_data.user_logspaces_size());
    DCHECK_EQ(n, index_data.user_tag_sizes_size());
    uint32_t total_tags = absl::c_accumulate(index_data.user_tag_sizes(), 0U);
    DCHECK_EQ(static_cast<int>(total_tags), index_data.user_tags_size());
    auto tag_iter = index_data.user_tags().begin();
    size_t num_stored_received_data = 0;
    size_t num_received_data = 0;
    for (int i = 0; i < n; i++) {
        size_t num_tags = index_data.user_tag_sizes(i);
        uint32_t seqnum = index_data.seqnum_halves(i);
        if (seqnum < indexed_seqnum_position_) {
            HVLOG_F(1, "Seqnum={} lower than IndexedSeqnumPosition={}", seqnum, indexed_seqnum_position_);
            tag_iter += num_tags;
            continue;
        }
        if (received_data_.count(seqnum) == 0) {
            auto tags = UserTagVec(tag_iter, tag_iter + num_tags);
            std::vector<uint64_t> filtered_tags;
            for(uint64_t tag : tags){
                if (min_tags_.contains(tag)){
                    if(is_my_turn){
                        filtered_tags.push_back(tag);
                    }
                } else {
                    min_tags_.insert(tag);
                    uint16_t index_shard = gsl::narrow_cast<uint16_t>(tag % view_->num_index_shards());
                    if(view_->GetIndexNode(my_index_node_id)->IsIndexShardMember(index_shard)){
                        filtered_tags.push_back(tag);
                    }
                }
            }
            if (0 < filtered_tags.size() || (tags.empty() && is_my_turn)) {
                received_data_[seqnum] = IndexData {
                    .engine_id     = gsl::narrow_cast<uint16_t>(index_data.engine_ids(i)),
                    .user_logspace = index_data.user_logspaces(i),
                    .user_tags     = UserTagVec(filtered_tags.begin(), filtered_tags.end()),
                    .skip = false
                };
                num_received_data++;
                num_stored_received_data++;
            } else {
                TryCreateIndex(index_data.user_logspaces(i));
                received_data_[seqnum] = IndexData {
                    .skip = true
                };
                num_received_data++;
            }
        }
        tag_iter += num_tags;
    }
    HVLOG_F(1, "From {} received seqnums there will be {} stored", num_received_data, num_stored_received_data);
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

void Index::PollQueryResults(QueryResultVec* results) {
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
    AdvanceIndexProgress();
}

void Index::AddCut(uint32_t metalog_seqnum, uint32_t next_seqnum) {
    cuts_.push_back(std::make_pair(metalog_seqnum, next_seqnum));
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

void Index::AdvanceIndexProgress() {
    HVLOG(1) << "Advance index progress";
    while (!cuts_.empty()) {
        uint32_t end_seqnum = cuts_.front().second;
        if (data_received_seqnum_position_ < end_seqnum) {
            HVLOG_F(1, "DataReceivedSeqnumPosition={} is lower then endSeqnum={} of cut.", bits::HexStr0x(data_received_seqnum_position_), bits::HexStr0x(end_seqnum));
            break;
        }
        HVLOG_F(1, "Apply IndexData until seqnum {}", bits::HexStr0x(end_seqnum));
        auto iter = received_data_.begin();
        while (iter != received_data_.end()) {
            uint32_t seqnum = iter->first;
            if (seqnum >= end_seqnum) {
                break;
            }
            const IndexData& index_data = iter->second;
            GetOrCreateIndex(index_data.user_logspace)->Add(
                seqnum, index_data.engine_id, index_data.user_tags);
            iter = received_data_.erase(iter);
        }
        DCHECK_GT(end_seqnum, indexed_seqnum_position_);
        indexed_seqnum_position_ = end_seqnum;
        uint32_t metalog_seqnum = cuts_.front().first;
        indexed_metalog_position_ = metalog_seqnum + 1;
        cuts_.pop_front();
    }
    if (!blocking_reads_.empty()) {
        int64_t current_timestamp = GetMonotonicMicroTimestamp();
        std::vector<std::pair<int64_t, IndexQuery>> unfinished;
        for (const auto& [start_timestamp, query] : blocking_reads_) {
            if (!ProcessBlockingQuery(query)) {
                if (current_timestamp - start_timestamp
                        < absl::ToInt64Microseconds(kBlockingQueryTimeout)) {
                    unfinished.push_back(std::make_pair(start_timestamp, query));
                } else {
                    pending_query_results_.push_back(BuildNotFoundResult(query));
                }
            }
        }
        blocking_reads_ = std::move(unfinished);
    }
    auto iter = pending_queries_.begin();
    while (iter != pending_queries_.end()) {
        if (iter->first > indexed_metalog_position_) {
            break;
        }
        const IndexQuery& query = iter->second;
        ProcessQuery(query);
        iter = pending_queries_.erase(iter);
    }
}

bool Index::AdvanceIndexProgress(const IndexDataProto& index_data, uint16_t my_index_node_id) {
    if(CheckIfNewIndexData(index_data)){
        HVLOG(1) << "IndexUpdate: New metalog progress";
        if(0 < index_data.seqnum_halves_size()){
            HVLOG(1) << "IndexUpdate: Contains index data";
            ProvideIndexData(index_data, my_index_node_id);
        }
    }
    bool advanced = false;
    uint32_t end_seqnum_position;
    while(TryCompleteIndexUpdates(&end_seqnum_position)){
        {
            auto iter = received_data_.begin();
            while (iter != received_data_.end()) {
                uint32_t seqnum = iter->first;
                if (end_seqnum_position <= seqnum){
                    break;
                }
                const IndexData& index_data = iter->second;
                if(!index_data.skip){
                    GetOrCreateIndex(index_data.user_logspace)->Add(
                        seqnum, index_data.engine_id, index_data.user_tags);
                }
                iter = received_data_.erase(iter);
            }
        }
        HVLOG_F(1, "IndexUpdate: Metalog increased. index_metalog_position={}", indexed_metalog_position_);
        if (!blocking_reads_.empty()) {
            int64_t current_timestamp = GetMonotonicMicroTimestamp();
            std::vector<std::pair<int64_t, IndexQuery>> unfinished;
            for (const auto& [start_timestamp, query] : blocking_reads_) {
                if (!ProcessBlockingQuery(query)) {
                    if (current_timestamp - start_timestamp
                            < absl::ToInt64Microseconds(kBlockingQueryTimeout)) {
                        unfinished.push_back(std::make_pair(start_timestamp, query));
                    } else {
                        pending_query_results_.push_back(BuildNotFoundResult(query));
                    }
                }
            }
            blocking_reads_ = std::move(unfinished);
        }
        auto iter = pending_queries_.begin();
        while (iter != pending_queries_.end()) {
            if (iter->first > indexed_metalog_position_) {
                break;
            }
            const IndexQuery& query = iter->second;
            ProcessQuery(query);
            iter = pending_queries_.erase(iter);
        }
        advanced = true;
    }
    return advanced;
}


bool Index::CheckIfNewIndexData(const IndexDataProto& index_data){
    bool index_data_new = false;
    for(int i = 0; i < index_data.meta_headers_size(); i++){
        auto meta_header = index_data.meta_headers().at(i);
        if (meta_header.metalog_position() <= metalog_position_){
            continue;
        }
        auto storage_shards = meta_header.my_active_storage_shards();
        size_t active_shards = meta_header.num_active_storage_shards();
        uint32_t metalog_position = meta_header.metalog_position();
        if(storage_shards_index_updates_.contains(metalog_position)){
            size_t before_update = storage_shards_index_updates_.at(metalog_position).second.size();
            storage_shards_index_updates_.at(metalog_position).second.insert(storage_shards.begin(), storage_shards.end());
            size_t after_update = storage_shards_index_updates_.at(metalog_position).second.size();
            DCHECK_GE(after_update, before_update);
            end_seqnum_positions_[metalog_position] = 
                std::min(end_seqnum_positions_.at(metalog_position), meta_header.end_seqnum_position());
            index_data_new = after_update > before_update; //check if some of the shards contributed
        } else {
            HVLOG_F(1, "Received new metalog_position={} for which {} shards are active", metalog_position, active_shards);
            storage_shards_index_updates_.insert({
                metalog_position,
                {
                    active_shards, // store the active shards for this metalog
                    absl::flat_hash_set<uint16_t>(storage_shards.begin(), storage_shards.end())
                }
            });
            DCHECK(0 < storage_shards_index_updates_.size());
            end_seqnum_positions_[metalog_position] = meta_header.end_seqnum_position();
            index_data_new = true;
        }
    }
    return index_data_new;
}

bool Index::TryCompleteIndexUpdates(uint32_t* end_seqnum_position){
    // check if next higher metalog_position is complete
    uint32_t next_index_metalog_position = indexed_metalog_position_ + 1;
    if (!storage_shards_index_updates_.contains(next_index_metalog_position)){
        HVLOG_F(1, "Metalog position {} not yet exists", next_index_metalog_position);
        return false;
    }
    auto entry = storage_shards_index_updates_.at(next_index_metalog_position);
    if (entry.first == 0) {
        HVLOG_F(1, "Number of shards for metalog position {} yet unknown", next_index_metalog_position);
        return false;
    }
    if (entry.first == entry.second.size()){
        // updates from all active storage shards received
        indexed_metalog_position_ = next_index_metalog_position;
        *end_seqnum_position = end_seqnum_positions_.at(next_index_metalog_position);
        storage_shards_index_updates_.erase(next_index_metalog_position);
        end_seqnum_positions_.erase(next_index_metalog_position);
        HVLOG_F(1, "Shards for metalog position {} completed", next_index_metalog_position);
        return true;
    }
    HVLOG_F(1, "Shards for metalog position {} not yet completed", next_index_metalog_position);
    return false;
}

Index::PerSpaceIndex* Index::GetOrCreateIndex(uint32_t user_logspace) {
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
        pending_query_results_.push_back(BuildNotFoundResult(query));
        HVLOG(1) << "IndexRead: ProcessReadNext: NotFoundResult";
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
                pending_query_results_.push_back(BuildNotFoundResult(query));
                HVLOG(1) << "IndexRead: ProcessReadNext: NotFoundResult";
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
        pending_query_results_.push_back(BuildNotFoundResult(query));
        HVLOG(1) << "IndexRead: ProcessReadPrev: NotFoundResult";
    }
}

bool Index::ProcessBlockingQuery(const IndexQuery& query) {
    DCHECK(query.direction == IndexQuery::kReadNextB && query.initial);
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    if (query_view_id > view_->id()) {
        pending_query_results_.push_back(BuildNotFoundResult(query));
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
    if (query.min_seqnum_query && (query.tail_seqnum == 0 || query.tail_seqnum < seqnum)){
        HVLOG_F(1, "Min query: seqnum result higher {} than tail {} of requester", bits::HexStr0x(seqnum), bits::HexStr0x(query.tail_seqnum));
        return IndexQueryResult {
            .state = IndexQueryResult::kFound,
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

IndexQueryResult Index::BuildNotFoundResult(const IndexQuery& query) {
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
