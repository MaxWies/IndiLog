#include "log/index_local_suffix.h"

#include "log/utils.h"

namespace faas {
namespace log {

SeqnumSuffixChain::SeqnumSuffixChain(uint16_t sequence_number_id, size_t max_suffix_seq_entries, float trim_level) 
    : sequence_number_id_(sequence_number_id),
      max_suffix_seq_entries_(max_suffix_seq_entries),
      trim_level_(trim_level),
      current_entries_(0)
    {
        log_header_ = fmt::format("SeqnumSuffixChain[{}]: ", sequence_number_id);
    }

SeqnumSuffixChain::~SeqnumSuffixChain() {}

void SeqnumSuffixChain::Extend(const View* view){
    uint16_t id = view->id();
    DCHECK(suffix_chain_.count(id) == 0);
    if (!IsEmpty()){
        DCHECK(suffix_chain_.begin()->first < id);
    }
    HVLOG_F(1, "Extend chain with link for view {}", id);
    suffix_chain_.emplace(id, std::make_unique<SeqnumSuffixLink>(view, sequence_number_id_));
}

void SeqnumSuffixChain::Trim(uint64_t bound){
    uint16_t view_id = bits::HighHalf32(bits::HighHalf64(bound));
    DCHECK_EQ(sequence_number_id_, bits::LowHalf32(bits::HighHalf64(bound)));
    if (IsEmpty()){
        return;
    }
    // remove older views
    auto it = suffix_chain_.begin();
    while(it != suffix_chain_.end()){
        if (it->first <= view_id){
            break;
        }
        it = suffix_chain_.erase(it);
    }
    if(view_id < it->first){
        // view gap possible
        return;
    }
    // remove entries within view
    suffix_chain_.at(view_id)->Trim(bound);
}

void SeqnumSuffixChain::Trim(size_t* counter){
    DCHECK(!IsEmpty());
    HVLOG_F(1, "Trim {} entries from chain", *counter);
    auto it = suffix_chain_.begin();
    while(it != suffix_chain_.end()){
        size_t entries = it->second->NumEntries();
        if(*counter < entries){
            break;
        }
        HVLOG_F(1, "Trim link {} completely", it->first);
        it = suffix_chain_.erase(it);
        *counter -= entries;
    }
    if(it != suffix_chain_.end()){
        it->second->Trim(counter);
    }
}

void SeqnumSuffixChain::ProvideMetaLog(const MetaLogProto& metalog_proto){
    if (IsEmpty()){
        return;
    }
    (--suffix_chain_.end())->second->ProvideMetaLog(metalog_proto);
    current_entries_++;
    auto iter = pending_queries_.begin();
    while (iter != pending_queries_.end()) {
        if (iter->first > metalog_position()) {
            break;
        }
        const IndexQuery& query = iter->second;
        pending_query_results_.push_back(ProcessQuery(query));
        iter = pending_queries_.erase(iter);
    }
    if (max_suffix_seq_entries_ < current_entries_){
        size_t counter = gsl::narrow_cast<size_t>(std::round(current_entries_ * trim_level_));
        size_t counter_copy = counter;
        HVLOG_F(1, "Trim op triggered. max_entries={}, current_entries={}, delete_counter={}", 
            max_suffix_seq_entries_, current_entries_, counter
        );
        DCHECK(counter <= current_entries_);
        Trim(&counter);
        DCHECK(counter == 0);
        current_entries_ -= counter_copy;
    }
}

void SeqnumSuffixChain::MakeQuery(const IndexQuery& query) {
    if(suffix_chain_.empty()){
        pending_query_results_.push_back(BuildNotFoundResult(query));
    }
    uint16_t query_view_id = log_utils::GetViewId(query.metalog_progress);
    if (query_view_id > view_id()){
        LOG(FATAL) << "Future view impossible";
    }
    if (query_view_id == view_id()){
        uint32_t position = bits::LowHalf64(query.metalog_progress);
        if (metalog_position() < position){
            HLOG_F(INFO, "Query with future metalog {}. My metalog_position is {}", position, metalog_position());
            pending_queries_.insert(std::make_pair(position, query));
            return;
        }
    }
    pending_query_results_.push_back(ProcessQuery(query));
}

void SeqnumSuffixChain::PollQueryResults(QueryResultVec* results) {
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

void SeqnumSuffixChain::Aggregate(size_t* link_entries, size_t* range_entries, size_t* size){
    for (const auto& chain_member : suffix_chain_){
        chain_member.second->Aggregate(link_entries, range_entries, size);      // member
        size += sizeof(uint16_t) * 1;                                           // key
    }
}

bool SeqnumSuffixChain::IsEmpty(){
    return suffix_chain_.empty();
}

bool SeqnumSuffixChain::GetHead(uint64_t* seqnum, uint16_t* storage_shard_id){
    for(auto& [view_id, entry] : suffix_chain_){
        if(!entry->IsEmpty()){
            entry->GetHead(seqnum, storage_shard_id);
            HVLOG_F(1, "SuffixRead: Suffix chain head at {}", bits::HexStr0x(*seqnum));
            return true;
        }
    }
    return false;
}

bool SeqnumSuffixChain::GetTail(uint64_t* seqnum, uint16_t* storage_shard_id){
    for(auto it = suffix_chain_.rbegin(); it != suffix_chain_.rend(); ++it){
        if(!it->second->IsEmpty()){
            it->second->GetTail(seqnum, storage_shard_id);
            HVLOG_F(1, "SuffixRead: Suffix chain tail at {}", bits::HexStr0x(*seqnum));
            return true;
        }
    }
    return false;
}

IndexQueryResult SeqnumSuffixChain::ProcessQuery(const IndexQuery& query){
    if (query.direction == IndexQuery::kReadNext){
        return ProcessReadNext(query);
    } else if (query.direction == IndexQuery::kReadPrev){
        return ProcessReadPrev(query);
    } else {
        UNREACHABLE();
    }
}

IndexQueryResult SeqnumSuffixChain::ProcessReadNext(const IndexQuery& query){
    uint64_t seqnum;
    uint16_t storage_shard_id;
    HVLOG_F(1, "SuffixRead: ProcessReadNext: seqnum={}, logspace={}, tag={}",
            bits::HexStr0x(query.query_seqnum), query.user_logspace, query.user_tag);
    if (IsEmpty()){
        HVLOG(1) << "SuffixRead: Chain is empty -> Empty";
        return BuildNotFoundResult(query);
    }
    // check if seqnum lies at head, left next to it or before
    if (GetHead(&seqnum, &storage_shard_id)){
        if (query.query_seqnum == seqnum){
            HVLOG(1) << "SuffixRead: Seqnum lies on head -> Found";
            return BuildFoundResult(query, view_id(), seqnum, storage_shard_id);
        }
        else if (query.query_seqnum < seqnum) {
            HVLOG(1) << "SuffixRead: Seqnum lies before head with gap -> Empty";
            return BuildNotFoundResult(query);
        }
    }
    // check if seqnum lies at tail or after tail in future
    if (GetTail(&seqnum, &storage_shard_id)){
        if (seqnum == query.query_seqnum){
            HVLOG(1) << "SuffixRead: Seqnum lies at tail -> Valid";
            return BuildFoundResult(query, view_id(), seqnum, storage_shard_id);
        }
        else if (seqnum < query.query_seqnum) {
            HVLOG(1) << "SuffixRead: Seqnum lies behind tail -> Invalid";
            return BuildInvalidResult(query);
        }
    }
    // invariant: seqnum lies within suffix head and tail
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    SeqnumSuffixLink* suffix_seq_lower = nullptr;
    auto it = suffix_chain_.lower_bound(query_view_id);
    // get lower
    while (it != suffix_chain_.end()){
        if(!it->second->IsEmpty()){
            suffix_seq_lower = it->second.get();
            ++it;
            break;
        }
        ++it;
    }
    if(suffix_seq_lower == nullptr) {
        // all empty
        HVLOG(1) << "All links empty -> Empty";
        return BuildNotFoundResult(query);
    }
    if(suffix_seq_lower->FindNext(query.query_seqnum, &seqnum, &storage_shard_id)){
        HVLOG(1) << "SuffixRead: Next seqnum in lower -> Found";
        return BuildFoundResult(
            query,
            view_id(), 
            seqnum,
            storage_shard_id
        );
    }
    // get upper
    SeqnumSuffixLink* suffix_seq_upper = nullptr;
    while (it != suffix_chain_.end()){
        if(!it->second->IsEmpty()){
            HVLOG(1) << "SuffixRead: Assign upper link";
            suffix_seq_upper = it->second.get();
            break;
        }
        ++it;
    }
    if(suffix_seq_upper == nullptr) {
        // create invalid result because seqnum lies in the future
        HVLOG(1) << "SuffixRead: Seqnum in future -> Invalid";
        return BuildInvalidResult(query);
    }
    // get first entry from an non-empty upper
    suffix_seq_upper->GetHead(&seqnum, &storage_shard_id);
    HVLOG(1) << "SuffixRead: First seqnum from upper -> Found";
    return BuildFoundResult(query, view_id(), seqnum, storage_shard_id);
}

IndexQueryResult SeqnumSuffixChain::ProcessReadPrev(const IndexQuery& query){
    uint64_t seqnum;
    uint16_t storage_shard_id;
    HVLOG_F(1, "SuffixRead: ProcessReadPrev: seqnum={}, logspace={}, tag={}",
            bits::HexStr0x(query.query_seqnum), query.user_logspace, query.user_tag);
    if (IsEmpty()) {
        HVLOG(1) << "SuffixRead: Chain is empty -> Empty";
        return BuildNotFoundResult(query);
    }
    // check if seqnum lies at head, left next to it or before
    if (GetHead(&seqnum, &storage_shard_id)){
        if (query.query_seqnum == seqnum){
            HVLOG(1) << "SuffixRead: Seqnum lies on head -> Found";
            return BuildFoundResult(query, view_id(), seqnum, storage_shard_id);
        }
        else if (query.query_seqnum < seqnum) {
            HVLOG(1) << "SuffixRead: Seqnum lies before head with gap -> Empty";
            return BuildNotFoundResult(query);
        }
    }
    // check if seqnum lies after tail
    if (GetTail(&seqnum, &storage_shard_id)){
        if (seqnum <= query.query_seqnum) {
            HVLOG(1) << "SuffixRead: Seqnum lies on or behind tail -> Found";
            return BuildFoundResult(query, view_id(), seqnum, storage_shard_id);
        }
    }
    // invariant: seqnum lies within suffix head and tail
    uint16_t query_view_id = log_utils::GetViewId(query.query_seqnum);
    SeqnumSuffixLink* suffix_link_upper = nullptr;
    auto it = suffix_chain_.lower_bound(query_view_id);
    // get upper
    while (it != suffix_chain_.begin()){
        if(!it->second->IsEmpty()){
            suffix_link_upper = it->second.get();
            break;
        }
        --it;
    }
    if(suffix_link_upper == nullptr && it == suffix_chain_.begin()){
        if(!it->second->IsEmpty()){
            suffix_link_upper = it->second.get();
        }
    }
    if(suffix_link_upper == nullptr) {
        // all empty
        HVLOG(1) << "All links empty -> Empty";
        return BuildNotFoundResult(query);
    }
    if(suffix_link_upper->FindPrev(query.query_seqnum, &seqnum, &storage_shard_id)){
        HVLOG(1) << "SuffixRead: Prev seqnum in upper -> Found";
        return BuildFoundResult(
            query,
            view_id(), 
            seqnum,
            storage_shard_id
        );
    }
    // lower not necessary because trimmed and contigous
    HVLOG(1) << "SuffixRead: Not in upper and lower is trimmed -> Empty";
    DCHECK_EQ(suffix_link_upper->view_id(), suffix_chain_.begin()->first);
    return BuildNotFoundResult(query);
}

IndexQueryResult SeqnumSuffixChain::BuildFoundResult(const IndexQuery& query, uint16_t view_id,
                                         uint64_t seqnum, uint16_t storage_shard_id) {
    return IndexQueryResult {
        .state = IndexQueryResult::kFound,
        .metalog_progress = metalog_progress(),
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = view_id,
            .storage_shard_id = storage_shard_id,
            .seqnum = seqnum
        }
    };
}

IndexQueryResult SeqnumSuffixChain::BuildNotFoundResult(const IndexQuery& query) {
    return IndexQueryResult {
        .state = IndexQueryResult::kEmpty,
        .metalog_progress = metalog_progress(),
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = kInvalidLogSeqNum
        }
    };
}

IndexQueryResult SeqnumSuffixChain::BuildInvalidResult(const IndexQuery& query) {
    return IndexQueryResult {
        .state = IndexQueryResult::kInvalid,
        .metalog_progress = metalog_progress(),
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = kInvalidLogSeqNum
        }
    };
}

SeqnumSuffixLink::SeqnumSuffixLink(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      first_metalog_(true)
    {
    log_header_ = fmt::format("SeqnumSuffixLink[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

SeqnumSuffixLink::~SeqnumSuffixLink() {}

namespace {
static inline uint16_t FindStorageShardIdInLinkEntry(uint32_t key_seqnum, const LinkEntry entry, uint32_t query_seqnum) {
    if (query_seqnum > key_seqnum) {
        LOG_F(WARNING, "Query_seqnum={} is higher than key_seqnum={} of LinkEntry. Result returned is not valid!", query_seqnum, key_seqnum);
        return entry.storage_shard_ids_.at(0);
    }
    uint8_t diff = gsl::narrow_cast<uint8_t>(key_seqnum-query_seqnum);
    auto it = std::upper_bound(entry.key_diffs_.begin(), entry.key_diffs_.end(), diff);
    if (it == entry.key_diffs_.end()){
        return entry.storage_shard_ids_.back();
    }
    size_t ix = gsl::narrow_cast<size_t>(std::distance(entry.key_diffs_.begin(), it));
    return entry.storage_shard_ids_.at(ix);
}
} // namespace

bool SeqnumSuffixLink::IsEmpty() {
    return entries_.empty();
}
void SeqnumSuffixLink::OnNewLogs(std::vector<std::pair<uint16_t, uint32_t>> productive_shards) {
    DCHECK(!productive_shards.empty());
    uint32_t seqnum_key = productive_shards.back().second;
    uint16_t highest_productive_shard = productive_shards.back().first;
    HVLOG_F(1, "New logs received. seqnum_key={}, prod_shards={}, highest_prod_shard={}", 
     bits::HexStr0x(seqnum_key), productive_shards.size(), highest_productive_shard
    );
    DCHECK(1 <= productive_shards.size());
    entries_.emplace_hint(
        entries_.end(),
        std::piecewise_construct,
        std::forward_as_tuple(seqnum_key),
        std::forward_as_tuple(productive_shards)
    );
}

void SeqnumSuffixLink::Trim(uint64_t bound){
    if (IsEmpty()){
        return;
    }
    DCHECK_EQ(bits::HighHalf32(bound), identifier());
    uint32_t local_bound = bits::LowHalf64(bound);
    // remove entries with keys until bound
    auto it = entries_.begin();
    while(it != entries_.end()){
        if (local_bound < it->first){
            break;
        }
        it = entries_.erase(it);
    }
}

void SeqnumSuffixLink::Trim(size_t* counter){
    if (IsEmpty()){
        return;
    }
    HVLOG_F(1, "Trim at most {} entries", *counter);
    auto it = entries_.begin();
    size_t c = *counter;
    size_t r = 0;
    while(r != c && it != entries_.end()){
        it = entries_.erase(it);
        r++;
    }
    *counter -= r;
}

size_t SeqnumSuffixLink::NumEntries(){
    return entries_.size();
}

void SeqnumSuffixLink::Aggregate(size_t* num_link_entries, size_t* num_range_entries, size_t* size){
    *num_link_entries += entries_.size();
    for(auto entry : entries_){
        *num_range_entries += entry.second.storage_shard_ids_.size();
        *size += (
              sizeof(uint32_t) * 1                                          // key
            + sizeof(uint16_t) * entry.second.storage_shard_ids_.size()     // productive shards
            + sizeof(uint8_t) * entry.second.key_diffs_.size()              // relative seqnums
        );
    }
}

void SeqnumSuffixLink::GetHead(uint64_t* seqnum, uint16_t* storage_shard_id){
    // the head is the bound of the lowest productive shard of the first entry
    DCHECK(!entries_.empty());
    uint32_t key = entries_.begin()->first;
    LinkEntry entry = entries_.begin()->second;
    if(0 < entry.key_diffs_.size()){
        *seqnum = bits::JoinTwo32(identifier(), key - uint32_t(entry.key_diffs_.back()));
        *storage_shard_id = entry.storage_shard_ids_.back();
    } else {
        *seqnum = bits::JoinTwo32(identifier(), key);
        *storage_shard_id = entry.storage_shard_ids_.front();
    }
}

void SeqnumSuffixLink::GetTail(uint64_t* seqnum, uint16_t* storage_shard_id){
    DCHECK(!entries_.empty());
    uint32_t key = (--entries_.end())->first;
    LinkEntry entry = (--entries_.end())->second;
    *seqnum = bits::JoinTwo32(identifier(), key);
    *storage_shard_id = entry.storage_shard_ids_.front();
    return;
}

bool SeqnumSuffixLink::FindNext(uint64_t query_seqnum, uint64_t* seqnum, uint16_t* storage_shard_id)
{
    DCHECK_EQ(view_id(), bits::HighHalf32(bits::HighHalf64(query_seqnum)));
    DCHECK_EQ(sequencer_id(), bits::LowHalf32(bits::HighHalf64(query_seqnum)));
    if (entries_.empty()){
        return false;
    }
    GetHead(seqnum, storage_shard_id);
    if (query_seqnum + 1 < *seqnum) {
        HVLOG(1) << ("SuffixRead: query seqnum before head -> empty");
        return false;
    }
    if (query_seqnum == *seqnum || query_seqnum + 1 == *seqnum) {
        HVLOG(1) << ("SuffixRead: query seqnum at my head -> found");
        return true;
    }
    GetTail(seqnum, storage_shard_id);
    if (*seqnum < query_seqnum) {
        HVLOG(1) << ("SuffixRead: Query seqnum behind tail -> empty");
        return false;
    }
    // invariant 1: seqnum lies between head and last element
    // invariant 2: seqnum has exact match (no closest to some other seqnum)
    uint32_t local_seqnum = bits::LowHalf64(query_seqnum);
    uint32_t entry_lower_key;
    LinkEntry* entry_lower = nullptr;
    LinkEntry* entry_upper = nullptr;
    auto it = entries_.upper_bound(local_seqnum);
    DCHECK(it != entries_.end()); // because of invariant
    uint32_t entry_upper_key = it->first;
    entry_upper = &it->second;
    if (it != entries_.begin() && it != entries_.end()){
        --it;
        entry_lower_key = it->first;
        entry_lower = &it->second;
    }
    // check if key match of lower happened
    if (entry_lower != nullptr && local_seqnum == entry_lower_key){
        *seqnum = query_seqnum;
        *storage_shard_id = entry_lower->storage_shard_ids_.front();
        HVLOG(1) << ("SuffixRead: Query seqnum matches a lower key -> found");
        return true;
    }
    *seqnum = query_seqnum;
    *storage_shard_id = FindStorageShardIdInLinkEntry(entry_upper_key, *entry_upper, local_seqnum);
    HVLOG(1) << ("SuffixRead: Query seqnum within my range -> found");
    return true;
}

bool SeqnumSuffixLink::FindPrev(uint64_t query_seqnum, uint64_t* seqnum, uint16_t* storage_shard_id){
    DCHECK_EQ(view_id(), bits::HighHalf32(bits::HighHalf64(query_seqnum)));
    DCHECK_EQ(sequencer_id(), bits::LowHalf32(bits::HighHalf64(query_seqnum)));
    if (entries_.empty()){
        return false;
    }
    GetHead(seqnum, storage_shard_id);
    if (query_seqnum < *seqnum) {
        HVLOG(1) << ("SuffixRead: Query seqnum before head -> empty");
        return false;
    }
    GetTail(seqnum, storage_shard_id);
    if (*seqnum <= query_seqnum) {
        HVLOG(1) << ("SuffixRead: Query seqnum on or behind my tail -> found");
        return true;
    }
    // invariant 1: seqnum lies between head and last element
    // invariant 2: seqnum has exact match (no closest to some other seqnum)
    uint32_t local_seqnum = bits::LowHalf64(query_seqnum);
    auto it = entries_.lower_bound(local_seqnum);
    DCHECK(it != entries_.end()); // because of invariant
    uint32_t key = it->first;
    LinkEntry entry = it->second;
    *seqnum = query_seqnum;
    *storage_shard_id = FindStorageShardIdInLinkEntry(key, entry, local_seqnum);
    HVLOG(1) << ("SuffixRead: Query seqnum within my range -> found");
    return true;
}


}
}