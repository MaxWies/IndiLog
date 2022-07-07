#include "log/index_local_tag_cache.h"

#include "log/utils.h"

namespace faas {
namespace log {

TagSuffixLink::TagSuffixLink(uint32_t seqnum, uint16_t storage_shard_id){
    seqnums_.push_back(seqnum);
    storage_shard_ids_.push_back(storage_shard_id);
}

bool TagSuffixLink::FindPrev(uint32_t identifier, uint64_t query_seqnum, uint64_t* seqnum, uint16_t* shard_id) const{
    DCHECK(!seqnums_.empty());
    uint32_t local_seqnum = bits::LowHalf64(query_seqnum);

    auto it = absl::c_upper_bound(
        seqnums_, local_seqnum, [] (uint32_t lhs, uint32_t rhs) {
            return lhs < rhs;
        }
    );

    if (it == seqnums_.begin()){
        return false;
    } else {
        --it;
        *seqnum = bits::JoinTwo32(identifier, *it);
        *shard_id = storage_shard_ids_.at(gsl::narrow_cast<size_t>(it - seqnums_.begin()));
        return true;
    }
}

void TagSuffixLink::GetTail(uint32_t identifier, uint64_t* seqnum, uint16_t* shard_id) const {
    DCHECK(!seqnums_.empty());
    *seqnum = bits::JoinTwo32(identifier, seqnums_.back());
    *shard_id = storage_shard_ids_.back();
}

bool TagSuffixLink::FindNext(uint32_t identifier, uint64_t query_seqnum, uint64_t* seqnum, uint16_t* shard_id) const {
    DCHECK(!seqnums_.empty());
    uint32_t local_seqnum = bits::LowHalf64(query_seqnum);
    auto it = absl::c_lower_bound(
        seqnums_, local_seqnum, [] (uint32_t lhs, uint32_t rhs) {
            return lhs < rhs;
        }
    );
    if(it == seqnums_.end()){
        return false;
    }
    *seqnum = bits::JoinTwo32(identifier, *it);
    *shard_id = storage_shard_ids_.at(gsl::narrow_cast<size_t>(it - seqnums_.begin()));       //it->second;
    return true;
}

void TagSuffixLink::GetHead(uint32_t identifier, uint64_t* seqnum, uint16_t* shard_id) const {
    DCHECK(!seqnums_.empty());
    *seqnum = bits::JoinTwo32(identifier, seqnums_.front());
    *shard_id = storage_shard_ids_.front();
}

TagEntry::TagEntry(uint64_t seqnum_min, uint16_t storage_shard_id_min, uint64_t popularity, bool complete)
    : seqnum_min_(seqnum_min),
      shard_id_min_(storage_shard_id_min),
      popularity_(popularity),
      complete_(complete)
    {}
TagEntry::TagEntry(uint16_t view_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity)
    : seqnum_min_(kInvalidLogSeqNum),
      shard_id_min_(0),
      popularity_(popularity),
      complete_(false)
    {
        TagSuffixLink* link = new TagSuffixLink(seqnum, storage_shard_id);
        tag_suffix_[view_id].reset(link);
    }

TagEntry::~TagEntry(){}

 void TagEntry::Add(uint16_t view_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity){
    auto tag_suffix = tag_suffix_.find(view_id);
    if(tag_suffix == tag_suffix_.end()){
        TagSuffixLink* link = new TagSuffixLink(seqnum, storage_shard_id);
        tag_suffix_[view_id].reset(link);
    } else {
        TagSuffixLink* link = tag_suffix_.at(view_id).get();
        link->seqnums_.push_back(seqnum);
        link->storage_shard_ids_.push_back(storage_shard_id);
    }
    popularity_ = popularity;
}

void TagEntry::Evict(uint32_t per_tag_seqnums_limit, size_t* num_evicted_seqnums){
    size_t erase_counter = 0;
    size_t num_seqnums_in_suffix = NumSeqnumsInSuffix();
    if (num_seqnums_in_suffix > per_tag_seqnums_limit){
        erase_counter = num_seqnums_in_suffix - per_tag_seqnums_limit;
    }
    auto it = tag_suffix_.begin();
    while (0 < erase_counter && it != tag_suffix_.end()) {
        TagSuffixLink* link = it->second.get();
        size_t seqnums_to_evict = std::min(erase_counter, link->seqnums_.size());
        link->seqnums_.erase(link->seqnums_.begin(), link->seqnums_.begin() + gsl::narrow_cast<int>(seqnums_to_evict));
        link->storage_shard_ids_.erase(link->storage_shard_ids_.begin(), link->storage_shard_ids_.begin() + gsl::narrow_cast<int>(seqnums_to_evict));
        complete_ = false;
        if (link->seqnums_.empty()){
            // link is empty
            it = tag_suffix_.erase(it);
        } else {
            ++it;
        }
        erase_counter -= seqnums_to_evict;
        *num_evicted_seqnums += seqnums_to_evict;
    }
}

void TagEntry::GetSuffixHead(uint16_t sequencer_id, uint64_t* seqnum, uint16_t* shard_id) const {
    DCHECK(!tag_suffix_.empty());
    uint16_t view_id = tag_suffix_.begin()->first;
    TagSuffixLink* link = tag_suffix_.begin()->second.get();
    DCHECK(!link->seqnums_.empty());
    *seqnum = bits::JoinTwo32(bits::JoinTwo16(view_id, sequencer_id), link->seqnums_.front());
    *shard_id = link->storage_shard_ids_.front();
}

void TagEntry::GetSuffixTail(uint16_t sequencer_id, uint64_t* seqnum, uint16_t* shard_id) const {
    DCHECK(!tag_suffix_.empty());
    uint16_t view_id = (--tag_suffix_.end())->first;
    TagSuffixLink *link = (--tag_suffix_.end())->second.get();
    DCHECK(!link->seqnums_.empty());
    *seqnum = bits::JoinTwo32(bits::JoinTwo16(view_id, sequencer_id), link->seqnums_.back());
    *shard_id = link->storage_shard_ids_.back();
}

size_t TagEntry::NumSeqnumsInSuffix() {
    size_t size = 0;
    for (auto& [view_id, suffix_link] : tag_suffix_){
        size += suffix_link->seqnums_.size();
    }
    return size;
}

PerSpaceTagCache::PerSpaceTagCache(uint32_t user_logspace)
    : user_logspace_(user_logspace)
    {
        log_header_ = fmt::format("PerSpaceTagCache[{}]: ", user_logspace % 1000);
    }

PerSpaceTagCache::~PerSpaceTagCache(){}

void PerSpaceTagCache::AddOrUpdate(uint64_t tag, uint16_t view_id, uint16_t sequencer_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity){
    if(TagEntryExists(tag)){
        HVLOG_F(1, "Add data to link={}: seqnum={}, shard_id={}", view_id, bits::HexStr0x(seqnum), storage_shard_id);
        tags_.at(tag)->Add(view_id, seqnum, storage_shard_id, popularity);
    } else {
        TagEntry* tag_entry = new TagEntry(view_id, seqnum, storage_shard_id, popularity);
        tags_[tag].reset(tag_entry);
    }
    if (pending_min_tags_.contains(tag)){
        TagEntry* stored_tag = tags_.at(tag).get();
        TagEntry* pending_tag = pending_min_tags_.at(tag).get();
        if (pending_tag->seqnum_min_ == kInvalidLogSeqNum) {
            // tag was new: suffix head is the min
            uint64_t min_seqnum;
            uint16_t min_storage_shard_id;
            stored_tag->GetSuffixHead(sequencer_id, &min_seqnum, &min_storage_shard_id);
            HVLOG_F(1, "Tag={} is updated with min_seqnum={} from its head.", tag, min_seqnum);
            stored_tag->seqnum_min_ = min_seqnum;
            stored_tag->shard_id_min_ = min_storage_shard_id;
        } else {
            // tag was not new
            HVLOG_F(1, "Tag={} is updated with min_seqnum={} from pending tag.", tag, pending_tag->seqnum_min_);
            stored_tag->seqnum_min_ = pending_tag->seqnum_min_;
            stored_tag->shard_id_min_ = pending_tag->shard_id_min_;
        }
        stored_tag->complete_ = pending_tag->complete_;
        pending_min_tags_.erase(tag);
    }
}

void PerSpaceTagCache::HandleMinSeqnum(uint64_t tag, uint64_t min_seqnum, uint16_t min_storage_shard_id, uint64_t timestamp, uint16_t sequencer_id, uint64_t popularity){
    if (min_seqnum == kInvalidLogSeqNum || timestamp <= min_seqnum){
        // the global min seqnum was invalid or is after the timestamp, thus the tag is new and complete
        HVLOG_F(1, "Tag={} is completely new", tag);
        if(tags_.contains(tag)){
            // head is min
            HVLOG_F(1, "min_seqnum for tag={} is overwritten with head", tag);
            uint64_t min_seqnum;
            uint16_t min_storage_shard_id;
            TagEntry* tag_entry = tags_.at(tag).get();
            tag_entry->GetSuffixHead(sequencer_id, &min_seqnum, &min_storage_shard_id);
            tag_entry->seqnum_min_ = min_seqnum;
            tag_entry->shard_id_min_ = min_storage_shard_id;
            tag_entry->complete_ = true;
        } else {
            // tag not yet exists, thus min seqnum for tag is pending
            if (!pending_min_tags_.contains(tag)){
                HVLOG_F(1, "min_seqnum for tag={} not existed before and not yet received. Tag is pending...", tag);
                TagEntry* tag_entry = new TagEntry(kInvalidLogSeqNum, min_storage_shard_id, popularity, /*complete*/ true);
                pending_min_tags_[tag].reset(tag_entry);
            }
        }
    } else if (min_seqnum != kInvalidLogSeqNum) {
        // min seqnum exists already
        HVLOG_F(1, "Tag={} exists already", tag);
        if(tags_.contains(tag)){
            TagEntry* tag_entry = tags_.at(tag).get();
            tag_entry->seqnum_min_ = min_seqnum;
            tag_entry->shard_id_min_ = min_storage_shard_id;
        } else {
            if (!pending_min_tags_.contains(tag)){
                TagEntry* tag_entry = new TagEntry(min_seqnum, min_storage_shard_id, popularity, /*complete*/ false);
                pending_min_tags_[tag].reset(tag_entry);
            }
        }
    } else {
        UNREACHABLE();
    }
}

bool PerSpaceTagCache::TagExists(uint64_t tag){
    return tags_.contains(tag);
}

void PerSpaceTagCache::Clear(){
    tags_.clear();
    pending_min_tags_.clear();
}

void PerSpaceTagCache::Evict(uint64_t popularity, uint32_t pet_tag_seqnums_limit, size_t* evicted_seqnums){
    auto it = tags_.begin();
    while (it != tags_.end()){
        if (it->second->popularity_ <= popularity){
            *evicted_seqnums += it->second->NumSeqnumsInSuffix();
            tags_.erase(it);
            if(pending_min_tags_.contains(it->first)) {
                pending_min_tags_.erase(it->first);
            }
        } else {
            // evict suffix at front if tag reached limit
            it->second->Evict(pet_tag_seqnums_limit, evicted_seqnums);
        }
        ++it;
    }
}

bool PerSpaceTagCache::TagEntryExists(uint64_t key){
    return tags_.count(key) > 0;
}

void PerSpaceTagCache::Aggregate(size_t* num_tags, size_t* num_seqnums, size_t* size){
    size_t num_min_seqnums = 0;
    size_t num_tag_suffix = 0;
    size_t num_suffix_seqnums = 0;
    for (auto& [key, tag_entry] : tags_){
        if (tag_entry->seqnum_min_ != kInvalidLogSeqNum){
            num_min_seqnums += 1;
        }
        num_tag_suffix += tag_entry->tag_suffix_.size();
        for (auto& [view_id, suffix_link] : tag_entry->tag_suffix_){
            num_suffix_seqnums += suffix_link->seqnums_.size();
        }
    }
    *num_tags += tags_.size();
    *num_seqnums += (num_min_seqnums + num_suffix_seqnums);
    *size += (
        (sizeof(uint64_t)                       // seqnum_min
        + sizeof(uint16_t)                      // storage_shard_id_min
        + sizeof(uint64_t)                      // popularity
        + sizeof(bool)                          // complete flag
        ) * tags_.size()
        + sizeof(uint16_t) * num_tag_suffix     // all keys of suffix entries
        + sizeof(uint32_t) * num_suffix_seqnums // all keys of suffix links
        + sizeof(uint16_t) * num_suffix_seqnums // all storage shard values of suffix links
    );
}

IndexQueryResult::State PerSpaceTagCache::FindPrev(uint64_t query_seqnum, uint64_t user_tag, uint16_t space_id, uint64_t popularity,
                                                   uint64_t* seqnum, uint16_t* storage_shard_id) {
    HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Start", user_tag, bits::HexStr0x(query_seqnum));
    if(!tags_.contains(user_tag)){
        HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Tag not in cache -> empty", user_tag, bits::HexStr0x(query_seqnum));
        return IndexQueryResult::kMiss;
    }
    uint16_t view_id = log_utils::GetViewId(query_seqnum);
    TagEntry* tag_entry = tags_.at(user_tag).get();
    // check after|at tail
    tag_entry->GetSuffixTail(space_id, seqnum, storage_shard_id);
    HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Tail at {}", user_tag, bits::HexStr0x(query_seqnum), bits::HexStr0x(*seqnum));
    if (*seqnum <= query_seqnum) {
        HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Query seqnum is at tail or after -> found", user_tag, bits::HexStr0x(query_seqnum));
        tag_entry->popularity_ = popularity;
        return IndexQueryResult::kFound;
    }
    // check before|at suffix head or at min seqnum
    tag_entry->GetSuffixHead(space_id, seqnum, storage_shard_id);
    HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Head at {}", user_tag, bits::HexStr0x(query_seqnum), bits::HexStr0x(*seqnum));
    if (query_seqnum == *seqnum) {
        HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Query seqnum lies on head -> found", user_tag, bits::HexStr0x(query_seqnum));
        tag_entry->popularity_ = popularity;
        return IndexQueryResult::kFound;
    }
    else if (query_seqnum < *seqnum) {
        if (tag_entry->complete_) {
            HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Query seqnum is before min seqnum -> empty", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kEmpty;
        }
        else if (tag_entry->seqnum_min_ != kInvalidLogSeqNum && tag_entry->seqnum_min_ == query_seqnum) {
            *seqnum = tag_entry->seqnum_min_;
            *storage_shard_id = tag_entry->shard_id_min_;
            HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): seqnum lies on min seqnum -> found", user_tag, bits::HexStr0x(query_seqnum));
            tag_entry->popularity_ = popularity;
            return IndexQueryResult::kFound;
        } else {
            HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Query seqnum is in gap -> miss", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kMiss;
        }
    }
    // invariant: seqnum lies within suffix head and suffix tail
    uint32_t id;
    uint16_t upper_view_id;
    TagSuffixLink* tag_suffix_link_upper = nullptr;
    auto it = std::make_reverse_iterator(tag_entry->tag_suffix_.lower_bound(view_id));
    // get upper
    if (it != tag_entry->tag_suffix_.rbegin()){
        upper_view_id = it->first;
        tag_suffix_link_upper = it->second.get();
        ++it;
    }
    if (tag_suffix_link_upper == nullptr){
        UNREACHABLE();
        return IndexQueryResult::kMiss;
    }
    id = bits::JoinTwo16(upper_view_id, space_id);
    if(tag_suffix_link_upper->FindPrev(id, query_seqnum, seqnum, storage_shard_id)){
        HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Seqnum in upper link suffix -> found", user_tag, bits::HexStr0x(query_seqnum));
        tag_entry->popularity_ = popularity;
        return IndexQueryResult::kFound;
    }
    if (it == tag_entry->tag_suffix_.rbegin()){
        UNREACHABLE();
        return IndexQueryResult::kMiss;
    }
    uint16_t lower_view_id;
    TagSuffixLink* tag_suffix_link_lower = nullptr;
    // get lower
    lower_view_id = it->first;
    tag_suffix_link_lower = it->second.get();
    if (tag_suffix_link_lower == nullptr) {
        UNREACHABLE();
        return IndexQueryResult::kMiss;
    }
    id = bits::JoinTwo16(lower_view_id, space_id);
    tag_suffix_link_lower->GetTail(id, seqnum, storage_shard_id);
    HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Seqnum at lower link suffix tail -> found", user_tag, bits::HexStr0x(query_seqnum));
    tag_entry->popularity_ = popularity;
    return IndexQueryResult::kFound;
}

IndexQueryResult::State PerSpaceTagCache::FindNext(uint64_t query_seqnum, uint64_t user_tag, uint16_t space_id, uint64_t popularity,
                                                   uint64_t* seqnum, uint16_t* storage_shard_id) {
    HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Start", user_tag, bits::HexStr0x(query_seqnum));
    if (!tags_.contains(user_tag)) {
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Tag not in cache -> miss", user_tag, bits::HexStr0x(query_seqnum));
        return IndexQueryResult::kMiss;
    }
    uint16_t view_id = log_utils::GetViewId(query_seqnum);
    TagEntry* tag_entry = tags_.at(user_tag).get();
    // check before|at suffix head or before|at min seqnum
    tag_entry->GetSuffixHead(space_id, seqnum, storage_shard_id);
    HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Head at {}", user_tag, bits::HexStr0x(query_seqnum), bits::HexStr0x(*seqnum));
    if (query_seqnum + 1 < *seqnum) {
        if (tag_entry->complete_){
            HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Seqnum lies between min seqnum and head for complete tag -> found", user_tag, bits::HexStr0x(query_seqnum));
            tag_entry->popularity_ = popularity;
            return IndexQueryResult::kFound;
        } else if (tag_entry->seqnum_min_ != kInvalidLogSeqNum && query_seqnum <= tag_entry->seqnum_min_) {
            *seqnum = tag_entry->seqnum_min_;
            *storage_shard_id = tag_entry->shard_id_min_;
            HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): seqnum equal|lower than min -> found", user_tag, bits::HexStr0x(query_seqnum));
            tag_entry->popularity_ = popularity;
            return IndexQueryResult::kFound;
        }
        else {
            //gap
            HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Seqnum lies before head -> miss", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kMiss;
        }
    } else if (query_seqnum == *seqnum) {
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Seqnum is on head -> found", user_tag, bits::HexStr0x(query_seqnum));
        tag_entry->popularity_ = popularity;
        return IndexQueryResult::kFound;
    }
    // check after tail
    tag_entry->GetSuffixTail(space_id, seqnum, storage_shard_id);
    HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Tail at {}", user_tag, bits::HexStr0x(query_seqnum), bits::HexStr0x(*seqnum));
    if (*seqnum == query_seqnum) {
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Query seqnum is at tail -> found", user_tag, bits::HexStr0x(query_seqnum));
        tag_entry->popularity_ = popularity;
        return IndexQueryResult::kFound;
    }
    if (*seqnum < query_seqnum) {
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Query seqnum is after tail -> empty", user_tag, bits::HexStr0x(query_seqnum));
        return IndexQueryResult::kEmpty;
    }
    // invariant: seqnum lies within suffix head and suffix tail
    uint32_t identifier;
    TagSuffixLink* tag_suffix_link = nullptr;
    auto it = tag_entry->tag_suffix_.lower_bound(view_id);
    // get lower
    if (it != tag_entry->tag_suffix_.end()){
        view_id = it->first;
        tag_suffix_link = it->second.get();
        ++it;
    }
    if (tag_suffix_link == nullptr){
        UNREACHABLE();
    }
    identifier = bits::JoinTwo16(view_id, space_id);
    if(tag_suffix_link->FindNext(identifier, query_seqnum, seqnum, storage_shard_id)){
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Seqnum in lower link suffix -> found", user_tag, bits::HexStr0x(query_seqnum), view_id);
        tag_entry->popularity_ = popularity;
        return IndexQueryResult::kFound;
    }
    // get upper
    tag_suffix_link = nullptr;
    if (it != tag_entry->tag_suffix_.end()){
        view_id = it->first;
        tag_suffix_link = it->second.get();
    }
    if(tag_suffix_link == nullptr) {
        UNREACHABLE();
    }
    // get head of next suffix entry
    identifier = bits::JoinTwo16(view_id, space_id);
    tag_suffix_link->GetHead(identifier, seqnum, storage_shard_id);
    HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Seqnum at upper link suffix head -> found", user_tag, bits::HexStr0x(query_seqnum));
    tag_entry->popularity_ = popularity;
    return IndexQueryResult::kFound;
}

TagCacheView::TagCacheView(uint16_t view_id, uint32_t metalog_position)
    : view_id_(view_id),
      metalog_position_(metalog_position),
      first_metalog_(true)
    {
        log_header_ = fmt::format("TagCacheView[{}]: ", view_id);
    }

TagCacheView::~TagCacheView() {}

bool TagCacheView::CheckIfNewIndexData(const IndexDataProto& index_data){
    bool index_data_new = false;
    if (index_data.metalog_position() <= metalog_position_){
        HVLOG_F(1, "Received metalog_position={} lower|equal my metalog_position={}", index_data.metalog_position(), metalog_position_);
        return false;
    }
    uint32_t metalog_position = index_data.metalog_position();
    if(storage_shards_index_updates_.contains(metalog_position)){
        HVLOG_F(1, "Received pending metalog_position={}. storage_shards_of_node={}, active_storage_shards={}", 
            metalog_position, index_data.my_productive_storage_shards_size(), index_data.num_productive_storage_shards()
        );
        size_t before_update = storage_shards_index_updates_.at(metalog_position).second.size();
        storage_shards_index_updates_.at(metalog_position).second.insert(
            index_data.my_productive_storage_shards().begin(), 
            index_data.my_productive_storage_shards().end()
        );
        size_t after_update = storage_shards_index_updates_.at(metalog_position).second.size();
        DCHECK_GE(after_update, before_update);
        index_data_new = after_update > before_update; //check if some of the shards contributed
    } else {
        HVLOG_F(1, "Received new metalog_position={}. storage_shards_of_node={}, active_storage_shards={}", 
            metalog_position, index_data.my_productive_storage_shards_size(), index_data.num_productive_storage_shards()
        );
        storage_shards_index_updates_.insert({
            metalog_position,
            {
                index_data.num_productive_storage_shards(), // store the productive shards for this metalog
                absl::flat_hash_set<uint16_t>(
                    index_data.my_productive_storage_shards().begin(), 
                    index_data.my_productive_storage_shards().end()
                )
            }
        });
        DCHECK(0 < storage_shards_index_updates_.size());
        end_seqnum_positions_[metalog_position] = index_data.end_seqnum_position();
        index_data_new = true;
    }
    return index_data_new;
}

bool TagCacheView::TryCompleteIndexUpdates(uint32_t* next_seqnum_position){
    // check if next higher metalog_position is complete
    uint32_t next_metalog_position = metalog_position_ + 1;
    if (!storage_shards_index_updates_.contains(next_metalog_position)){
        HVLOG_F(1, "Metalog position {} not yet exists", next_metalog_position);
        return false;
    }
    auto entry = storage_shards_index_updates_.at(next_metalog_position);
    if (entry.first == 0) {
        HVLOG_F(1, "Number of shards for metalog position {} yet unknown", next_metalog_position);
        return false;
    }
    if (entry.first == entry.second.size()){
        // updates from all active storage shards received
        metalog_position_ = next_metalog_position;
        *next_seqnum_position = end_seqnum_positions_.at(next_metalog_position);
        storage_shards_index_updates_.erase(next_metalog_position);
        end_seqnum_positions_.erase(next_metalog_position);
        HVLOG_F(1, "Shards for metalog position {} completed", next_metalog_position);
        return true;
    }
    HVLOG_F(1, "Shards for metalog position {} not yet completed", next_metalog_position);
    return false;
}

TagCache::TagCache(uint16_t sequencer_id, size_t max_cache_size, uint32_t per_tag_seqnums_limit)
    : sequencer_id_(sequencer_id),
      max_cache_size_(max_cache_size),
      per_tag_seqnums_limit_(per_tag_seqnums_limit),
      cache_size_(0)
    {
        log_header_ = fmt::format("TagCache[{}]: ", sequencer_id);
    }

TagCache::~TagCache() {}

void TagCache::ProvideIndexData(uint16_t view_id, const IndexDataProto& index_data, std::vector<PendingMinTag>& pending_min_tags){
    HVLOG(1) << "Provide index data";
    if (!pending_min_tags.empty()){
        uint64_t popularity = bits::JoinTwo32(latest_view_id_, latest_metalog_position());
        for (auto pending_min_tag : pending_min_tags) {
            GetOrCreatePerSpaceTagCache(pending_min_tag.user_logspace)->HandleMinSeqnum(
                pending_min_tag.tag,
                pending_min_tag.seqnum,
                pending_min_tag.storage_shard_id,
                pending_min_tag.timestamp,
                sequencer_id_,
                popularity
            );
        }
    }
    if (!views_.at(view_id)->CheckIfNewIndexData(index_data)) {
        return;
    }
    HVLOG(1) << "Receive new index data";
    int n = index_data.seqnum_halves_size();
    DCHECK_EQ(n, index_data.engine_ids_size());
    DCHECK_EQ(n, index_data.user_logspaces_size());
    DCHECK_EQ(n, index_data.user_tag_sizes_size());
    uint32_t total_tags = absl::c_accumulate(index_data.user_tag_sizes(), 0U);
    DCHECK_EQ(static_cast<int>(total_tags), index_data.user_tags_size());
    auto tag_iter = index_data.user_tags().begin();
    for (int i = 0; i < n; i++) {
        size_t num_tags = index_data.user_tag_sizes(i);
        if (num_tags < 1){
            // tag cache stores only seqnums with tags
            continue;
        }
        uint32_t seqnum = index_data.seqnum_halves(i);
        received_data_[seqnum] = IndexData {
            .engine_id     = gsl::narrow_cast<uint16_t>(index_data.engine_ids(i)),
            .user_logspace = index_data.user_logspaces(i),
            .user_tags     = UserTagVec(tag_iter, tag_iter + num_tags)
        };
        tag_iter += num_tags;
    }
}

// void TagCache::ProvideMinSeqnumData(uint32_t user_logspace, uint64_t tag, const IndexResultProto& index_result_proto){
//     GetOrCreatePerSpaceTagCache(user_logspace)->HandleMinSeqnum(
//         tag,
//         index_result_proto.seqnum(),
//         gsl::narrow_cast<uint16_t>(index_result_proto.storage_shard_id()),
//         sequencer_id_,
//         bits::JoinTwo32(latest_view_id_, latest_metalog_position())
//     );
// }

void TagCache::Evict(){
    while(cache_size_ > max_cache_size_){
        HVLOG_F(1, "Evict because cache size too high. cache_size={}, max_cache_size={}", cache_size_, max_cache_size_);
        if (popularity_sequence_.empty()) {
            LOG(FATAL) << "Popularity sequence cannot be empty when cache gets evicted";
        }
        size_t evicted_seqnums = 0;
        size_t ix = gsl::narrow_cast<size_t>(std::ceil(popularity_sequence_.size() * 0.2));
        uint64_t popularity = popularity_sequence_.at(ix);
        for(auto& [user_logspace, per_space_tag_cache] : per_space_cache_) {
            per_space_tag_cache->Evict(popularity, per_tag_seqnums_limit_, &evicted_seqnums);
        }
        popularity_sequence_.erase(popularity_sequence_.begin(), popularity_sequence_.begin()+gsl::narrow_cast<int>(ix)+1);
        DCHECK(cache_size_ >= evicted_seqnums);
        cache_size_ -= evicted_seqnums;
        HVLOG_F(1, "Evicted {} seqnums. cache_size={}", evicted_seqnums, cache_size_);
    }
}

bool TagCache::TagExists(uint32_t user_logspace, uint64_t tag){
    return GetOrCreatePerSpaceTagCache(user_logspace)->TagExists(tag);
}

void TagCache::InstallView(uint16_t view_id, uint32_t metalog_position){
    DCHECK(!views_.contains(view_id));
    TagCacheView* tag_cache_view = new TagCacheView(view_id, metalog_position);
    views_[view_id].reset(tag_cache_view);
    latest_view_id_ = view_id;
}

void TagCache::Clear(){
    for (auto& [id, per_space_tag_cache] : per_space_cache_){
        per_space_tag_cache->Clear();
    }
    popularity_sequence_.clear();
    cache_size_ = 0;
}

void TagCache::Aggregate(size_t* num_tags, size_t* num_seqnums, size_t* size){
    for (auto& [user_logspace, per_space_tag_cache] : per_space_cache_){
        per_space_tag_cache->Aggregate(num_tags, num_seqnums, size);
        *size += (
            sizeof(uint32_t) * 1        // user_logspace key
        );
    }
}

bool TagCache::AdvanceIndexProgress(uint16_t view_id){
    bool advanced = false;
    uint32_t end_seqnum_position;
    while (views_.at(view_id)->TryCompleteIndexUpdates(&end_seqnum_position)){
        size_t new_seqnums = 0;
        uint64_t popularity = bits::JoinTwo32(latest_view_id_, latest_metalog_position());
        {
            auto iter = received_data_.begin();
            while (iter != received_data_.end()) {
                uint32_t seqnum = iter->first;
                if (end_seqnum_position <= seqnum){
                    break;
                }
                const IndexData& index_data = iter->second;
                for (uint64_t tag : index_data.user_tags) {
                    GetOrCreatePerSpaceTagCache(index_data.user_logspace)->AddOrUpdate(
                        tag,
                        view_id,
                        sequencer_id_,
                        seqnum,
                        index_data.engine_id,
                        popularity
                    );
                    new_seqnums++;
                }
                iter = received_data_.erase(iter);
            }
        }
        advanced = true;
        cache_size_ += new_seqnums;
        popularity_sequence_.push_back(popularity);
        HVLOG_F(1, "Advanced metalog to {}", views_.at(view_id)->metalog_position());
    }
    if (advanced) {
        auto iter = pending_queries_.begin();
        while (iter != pending_queries_.end()) {
            if (iter->first > latest_metalog_position()) {
                break;
            }
            const IndexQuery& query = iter->second;
            ProcessQuery(query);
            iter = pending_queries_.erase(iter);
        }
        Evict();
    }
    return advanced;
}

// void TagCache::OnFinalized(uint32_t metalog_position) {
//     auto iter = pending_queries_.begin();
//     while (iter != pending_queries_.end()) {
//         DCHECK_EQ(iter->first, kMaxMetalogPosition);
//         const IndexQuery& query = iter->second;
//         ProcessQuery(query);
//         iter = pending_queries_.erase(iter);
//     }
// }

// bool TagCache::Finalize(uint32_t final_metalog_position,
//                             const std::vector<MetaLogProto>& tail_metalogs) {
//                                 return true;
//                             }

//TODO: on finalized

PerSpaceTagCache* TagCache::GetOrCreatePerSpaceTagCache(uint32_t user_logspace){
    if (per_space_cache_.contains(user_logspace)) {
        return per_space_cache_.at(user_logspace).get();
    }
    PerSpaceTagCache* cache = new PerSpaceTagCache(user_logspace);
    per_space_cache_[user_logspace].reset(cache);
    return cache;
}

void TagCache::MakeQuery(const IndexQuery& query){
    uint16_t view_id = log_utils::GetViewId(query.metalog_progress);
    // query view higher
    if (view_id > latest_view_id_) {
        LOG(FATAL) << "Future view impossible";
    }
    // query view same
    else if (view_id == latest_view_id_){
        uint32_t position = bits::LowHalf64(query.metalog_progress);
        if (position <= latest_metalog_position()) {
            HVLOG(1) << "IndexRead: Can process query. Metalog in index equal or higher.";
            ProcessQuery(query);
        } else {
            HVLOG_F(1, "IndexRead: Query has higher metalog position: Query metalog_progress={}, Index metalog_progress={}. Add query to pending queries.", 
                position, latest_metalog_position());
            pending_queries_.insert(std::make_pair(position, query));
        }
    }
    // query view lower
    else {
        DCHECK_LT(view_id, latest_view_id_);
        ProcessQuery(query);
    }
}

void TagCache::PollQueryResults(QueryResultVec* results) {
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

void TagCache::ProcessQuery(const IndexQuery& query){
    uint64_t progress = index_metalog_progress();
    if (!per_space_cache_.contains(query.user_logspace)){
        LOG(WARNING) << "Unknown logspace";
        pending_query_results_.push_back(BuildMissResult(query, progress));
        return;
    }
    uint64_t popularity = bits::JoinTwo32(latest_view_id_, latest_metalog_position());
    PerSpaceTagCache* cache = GetOrCreatePerSpaceTagCache(query.user_logspace);
    uint64_t seqnum;
    uint16_t storage_shard_id;
    IndexQueryResult::State state;
    if (query.direction == IndexQuery::kReadNext) {
        state = cache->FindNext(
            query.query_seqnum,
            query.user_tag,
            sequencer_id_,
            popularity,
            &seqnum,
            &storage_shard_id
        );
    } else if (query.direction == IndexQuery::kReadPrev) {
        state = cache->FindPrev(
            query.query_seqnum,
            query.user_tag,
            sequencer_id_,
            popularity,
            &seqnum,
            &storage_shard_id
        );
    } else {
        state = IndexQueryResult::kEmpty;
        UNREACHABLE();
    }
    switch(state){
        case IndexQueryResult::kFound:
            pending_query_results_.push_back(BuildFoundResult(query, progress, latest_view_id_, seqnum, storage_shard_id));
            return;
        case IndexQueryResult::kMiss:
            pending_query_results_.push_back(BuildMissResult(query, progress));
            return;
        case IndexQueryResult::kEmpty:
            pending_query_results_.push_back(BuildEmptyResult(query, progress));
            return;
        case IndexQueryResult::kContinue:
            LOG(FATAL) << "No need for kContinue";
            return;
    }
}

IndexQueryResult TagCache::BuildFoundResult(const IndexQuery& query, uint64_t metalog_progress, uint16_t view_id,
                                         uint64_t seqnum, uint16_t storage_shard_id) {
    return IndexQueryResult {
        .state = IndexQueryResult::kFound,
        .metalog_progress = metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = view_id,
            .storage_shard_id = storage_shard_id,
            .seqnum = seqnum
        }
    };
}

IndexQueryResult TagCache::BuildMissResult(const IndexQuery& query, uint64_t metalog_progress) {
    return IndexQueryResult {
        .state = IndexQueryResult::kMiss,
        .metalog_progress = metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = kInvalidLogSeqNum
        }
    };
}

IndexQueryResult TagCache::BuildEmptyResult(const IndexQuery& query, uint64_t metalog_progress) {
    return IndexQueryResult {
        .state = IndexQueryResult::kEmpty,
        .metalog_progress = metalog_progress,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = kInvalidLogSeqNum
        }
    };
}

}
}