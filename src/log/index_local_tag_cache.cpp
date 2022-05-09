#include "log/index_local_tag_cache.h"

#include "log/utils.h"

namespace faas {
namespace log {

TagEntry::TagEntry(uint64_t seqnum_min, uint16_t storage_shard_id_min, uint64_t popularity)
    : seqnum_min_(seqnum_min),
      shard_id_min_(storage_shard_id_min),
      popularity_(popularity)
    {}
TagEntry::TagEntry(TagSuffix tag_suffix, uint64_t popularity)
    : tag_suffix_(tag_suffix.begin(), tag_suffix.end()),
      seqnum_min_(kInvalidLogSeqNum),
      shard_id_min_(0),
      popularity_(popularity)
    {}
TagEntry::TagEntry(uint16_t view_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity)
    : seqnum_min_(kInvalidLogSeqNum),
      shard_id_min_(0),
      popularity_(popularity)
    {
        //tag_suffix_.emplace_hint(tag_suffix_.end(), view_id, TagSuffixLink{{seqnum, storage_shard_id}});
        //tag_suffix_.at(view_id)
        tag_suffix_.insert(std::make_pair(view_id, std::map<uint32_t, uint16_t>{std::make_pair(seqnum, storage_shard_id)}));
    }
 void TagEntry::Add(uint16_t view_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity){
    auto tag_suffix = tag_suffix_.find(view_id);
    if(tag_suffix == tag_suffix_.end()){
        tag_suffix_.insert(std::make_pair(view_id, std::map<uint32_t, uint16_t>{std::make_pair(seqnum, storage_shard_id)}));
        //entry->tag_suffix_.emplace_hint(entry->tag_suffix_.end(), view_id, TagSuffixLink{seqnum, storage_shard_id});
    } else {
        LOG(INFO) << "Append data to existing suffix";
        tag_suffix_.at(view_id).insert({seqnum, storage_shard_id});
        //tag_suffix_link.emplace_hint(tag_suffix_link.end(), seqnum, storage_shard_id);
    }
    popularity_ = popularity;
}
TagEntry::~TagEntry(){}

PerSpaceTagCache::PerSpaceTagCache(uint32_t user_logspace)
    : user_logspace_(user_logspace)
    {
        log_header_ = fmt::format("PerSpaceTagCache[{}]: ", user_logspace % 1000);
    }

PerSpaceTagCache::~PerSpaceTagCache(){}

namespace {
// static inline void TagEntryAddData(TagEntry* entry, uint16_t view_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity) {
//     auto tag_suffix = entry->tag_suffix_.find(view_id);
//     if(tag_suffix == entry->tag_suffix_.end()){
//         entry->tag_suffix_.insert(std::make_pair(view_id, std::map<uint32_t, uint16_t>{std::make_pair(seqnum, storage_shard_id)}));
//         //entry->tag_suffix_.emplace_hint(entry->tag_suffix_.end(), view_id, TagSuffixLink{seqnum, storage_shard_id});
//     } else {
//         LOG(INFO) << "Append data to existing suffix";
//         auto tag_suffix_link = entry->tag_suffix_.at(view_id);
//         tag_suffix_link.insert({seqnum, storage_shard_id});
//         LOG_F(INFO, "1 sshid{}", entry->tag_suffix_.at(view_id).at(seqnum));
//         //tag_suffix_link.emplace_hint(tag_suffix_link.end(), seqnum, storage_shard_id);
//     }
//     LOG_F(INFO, "2 sshid{}", entry->tag_suffix_.at(view_id).at(seqnum));
//     entry->popularity_ = popularity;
// }
// do not delete
// static inline void TagEntryAddData(TagEntry entry, uint64_t seqnum_min, uint16_t shard_id_min, uint64_t popularity) {
//     DCHECK(entry.seqnum_min_ == kInvalidLogSeqNum);
//     entry.seqnum_min_ = seqnum_min;
//     entry.shard_id_min_ = shard_id_min;
//     entry.popularity_ = popularity;
// }
static inline bool TagSuffixGetHead(const TagSuffix& suffix, uint16_t sequencer_id, uint64_t* seqnum, uint16_t* shard_id){
    if (suffix.empty()){
        return false;
    }
    for(auto it = suffix.begin(); it != suffix.end(); ++it){
        if(!it->second.empty()){
            uint16_t view_id = it->first;
            TagSuffixLink link = it->second;
            DCHECK(!link.empty());
            *seqnum = bits::JoinTwo32(bits::JoinTwo16(view_id, sequencer_id), link.begin()->first);
            *shard_id = link.begin()->second;
            LOG_F(INFO, "Suffix head at {}", bits::HexStr0x(*seqnum));
            return true;
        } else {
            LOG_F(WARNING, "Tag suffix has empty link {}", it->first);
        }
    }
    return false;
}
static inline bool TagSuffixGetTail(const TagSuffix& suffix, uint16_t sequencer_id, uint64_t* seqnum, uint16_t* shard_id){
    if (suffix.empty()){
        return false;
    }
    for(auto it = suffix.rbegin(); it != suffix.rend(); ++it){
        if(!it->second.empty()){
            uint16_t view_id = it->first;
            TagSuffixLink link = it->second;
            DCHECK(!link.empty());
            *seqnum = bits::JoinTwo32(bits::JoinTwo16(view_id, sequencer_id), (--link.end())->first);
            *shard_id = (--link.end())->second;
            LOG_F(INFO, "Suffix tail at {}", bits::HexStr0x(*seqnum));
            LOG_F(INFO, "Length of tail link {}", link.size());
            return true;
        } else {
            LOG_F(WARNING, "Tag suffix has empty link {}", it->first);
        }
    }
    return false;
}
static inline bool TagSuffixLinkFindPrev(const TagSuffixLink& link, uint32_t identifier, uint64_t query_seqnum, uint64_t* seqnum, uint16_t* shard_id){
    DCHECK(!link.empty());
    uint32_t local_seqnum = bits::LowHalf64(query_seqnum);
    auto it = link.lower_bound(local_seqnum);
    if(it == link.begin()){
        if (it->first < local_seqnum){
            LOG_F(INFO, "TagSuffixLinkPrev: Tag Link {} seqnums higher than seqnum={}", bits::HighHalf32(identifier), bits::HexStr0x(local_seqnum));
            return false;
        }
    }
    if(it == link.end()){
        --it;
    }
    *seqnum = bits::JoinTwo32(identifier, it->first);
    *shard_id = it->second;
    return true;
}
static inline bool TagSuffixLinkGetTail(const TagSuffixLink& link, uint32_t identifier, uint64_t* seqnum, uint16_t* shard_id){
    DCHECK(!link.empty());
    *seqnum = bits::JoinTwo32(identifier, (--link.end())->first);
    *shard_id = (--link.end())->second;
    LOG_F(INFO, "Tag Link {} tail at seqnum={}", bits::HighHalf32(identifier), bits::HexStr0x(*seqnum));
    return true;
}
static inline bool TagSuffixLinkFindNext(const TagSuffixLink& link, uint32_t identifier, uint64_t query_seqnum, uint64_t* seqnum, uint16_t* shard_id){
    DCHECK(!link.empty());
    uint32_t local_seqnum = bits::LowHalf64(query_seqnum);
    auto it = link.lower_bound(local_seqnum);
    if(it == link.end()){
        LOG_F(INFO, "TagSuffixLinkNext: Tag Link {} seqnums lower than seqnum={}", bits::HighHalf32(identifier), bits::HexStr0x(local_seqnum));
        uint64_t t;
        uint16_t s;
        TagSuffixLinkGetTail(link, identifier, &t, &s);
        return false;
    }
    *seqnum = bits::JoinTwo32(identifier, it->first);
    *shard_id = it->second;
    return true;
}
static inline bool TagSuffixLinkGetHead(const TagSuffixLink& link, uint32_t identifier, uint64_t* seqnum, uint16_t* shard_id){
    DCHECK(!link.empty());
    *seqnum = bits::JoinTwo32(identifier, link.begin()->first);
    *shard_id = link.begin()->second;
    LOG_F(INFO, "Tag Link {} head at seqnum={}", bits::HighHalf32(identifier), bits::HexStr0x(*seqnum));
    return true;
}
} // namespace

void PerSpaceTagCache::AddOrUpdate(uint64_t tag, uint16_t view_id, uint16_t sequencer_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity){
    if(TagEntryExists(tag)){
        HVLOG_F(1, "Add data to link={}: seqnum={}, shard_id={}", view_id, bits::HexStr0x(seqnum), storage_shard_id);
        uint64_t seqnum_before;
        uint64_t seqnum_after;
        uint16_t shard_id;
        TagSuffixGetTail(tags_.at(tag)->tag_suffix_, 0, &seqnum_before, &shard_id);
        tags_.at(tag)->Add(view_id, seqnum, storage_shard_id, popularity);
        TagSuffixGetTail(tags_.at(tag)->tag_suffix_, 0, &seqnum_after, &shard_id);
    } else {
        TagEntry* tag_entry = new TagEntry(view_id, seqnum, storage_shard_id, popularity);
        tags_[tag].reset(tag_entry);
    }
    if (tags_.at(tag)->seqnum_min_ == kInvalidLogSeqNum) {
        uint64_t min_seqnum;
        uint16_t min_storage_shard_id;
        TagSuffixGetHead(tags_.at(tag)->tag_suffix_, sequencer_id, &min_seqnum, &min_storage_shard_id);
        HandleMinSeqnum(tag, min_seqnum, storage_shard_id);
    }
}

void PerSpaceTagCache::HandleMinSeqnum(uint64_t tag, uint64_t min_seqnum, uint16_t min_storage_shard_id){
    if(pending_seqnum_min_.contains(tag)){
        if (min_seqnum < pending_seqnum_min_.at(tag).first){
            tags_.at(tag)->seqnum_min_ = min_seqnum;
            tags_.at(tag)->shard_id_min_ = min_storage_shard_id;
        } else {
            tags_.at(tag)->seqnum_min_ = pending_seqnum_min_.at(tag).first;
            tags_.at(tag)->shard_id_min_ = pending_seqnum_min_.at(tag).second;
        }
        HVLOG_F(1, "Inserted min_seqnum={} for tag={}", tags_.at(tag)->seqnum_min_, tag);
        pending_seqnum_min_.erase(tag);
    } else {
        pending_seqnum_min_.insert({tag, {min_seqnum, min_storage_shard_id}});
    }
}

bool PerSpaceTagCache::TagExists(uint64_t tag){
    return tags_.contains(tag);
}

void PerSpaceTagCache::Remove(uint64_t tag, uint64_t popularity){
    if(tags_.at(tag)->popularity_ <= popularity){
        tags_.erase(tag);
    }
}

void PerSpaceTagCache::Remove(uint64_t popularity){
    auto iter = tags_.begin();
    while (iter != tags_.end()){
        if (iter->second->popularity_ <= popularity){
            tags_.erase(iter);
        }
        ++iter;
    }
}

void PerSpaceTagCache::UpdatePopularity(uint64_t tag, uint64_t popularity){
    DCHECK(tags_.contains(tag));
    tags_.at(tag)->popularity_ = popularity;
}

bool PerSpaceTagCache::TagEntryExists(uint64_t key){
    return tags_.count(key) > 0;
}

IndexQueryResult::State PerSpaceTagCache::FindPrev(uint64_t query_seqnum, uint64_t user_tag, uint16_t space_id,
                                                   uint64_t* seqnum, uint16_t* storage_shard_id) const {
    HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Start", user_tag, bits::HexStr0x(query_seqnum));
    if(!tags_.contains(user_tag)){
        HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Tag not in cache -> empty", user_tag, bits::HexStr0x(query_seqnum));
        return IndexQueryResult::kEmpty;
    }
    uint16_t view_id = log_utils::GetViewId(query_seqnum);
    TagEntry tag_entry = *tags_.at(user_tag).get();
    // check before min seqnum
    if (tag_entry.seqnum_min_ != kInvalidLogSeqNum){
        if (query_seqnum < tag_entry.seqnum_min_) {
            HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): seqnum lower min -> invalid", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kInvalid;
        } else if (query_seqnum == tag_entry.seqnum_min_) {
            *seqnum = tag_entry.seqnum_min_;
            *storage_shard_id = tag_entry.shard_id_min_;
            HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): seqnum equal min -> found", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kFound;
        }
    }
    // check before suffix head
    if (TagSuffixGetHead(tag_entry.tag_suffix_, space_id, seqnum, storage_shard_id)){
        if (query_seqnum < *seqnum) {
            HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Query seqnum is before head -> empty", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kEmpty;
        } else if (query_seqnum == *seqnum) {
            HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Query seqnum lies on head -> found", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kFound;
        }
    } else {
        // if head is empty all is empty
        HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): No head exists, all empty -> empty", user_tag, bits::HexStr0x(query_seqnum));
        return IndexQueryResult::kEmpty;
    }
    // check after tail
    if (TagSuffixGetTail(tag_entry.tag_suffix_, space_id, seqnum, storage_shard_id)){
        if (*seqnum <= query_seqnum) {
            HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Query seqnum is at tail or after -> found", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kFound;
        }
    }
    // invariant: seqnum lies between suffix head and suffix tail
    uint32_t id;
    uint16_t upper_view_id;
    TagSuffixLink* tag_suffix_link_upper = nullptr;
    auto it = std::make_reverse_iterator(tag_entry.tag_suffix_.lower_bound(view_id));
    // get lower
    while (it != tag_entry.tag_suffix_.rbegin()){
        if(!it->second.empty()){
            upper_view_id = it->first;
            tag_suffix_link_upper = &it->second;
            ++it;
            break;
        }
        ++it;
    }
    if (tag_suffix_link_upper == nullptr){
        UNREACHABLE();
        return IndexQueryResult::kEmpty;
    }
    id = bits::JoinTwo16(upper_view_id, space_id);
    if(TagSuffixLinkFindPrev(*tag_suffix_link_upper, id, query_seqnum, seqnum, storage_shard_id)){
        HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Seqnum in upper link suffix -> found", user_tag, bits::HexStr0x(query_seqnum));
        return IndexQueryResult::kFound;
    }
    if (it == tag_entry.tag_suffix_.rbegin()){
        UNREACHABLE();
        return IndexQueryResult::kEmpty;
    }
    uint16_t lower_view_id;
    TagSuffixLink* tag_suffix_link_lower = nullptr;
    // get lower
    while (it != tag_entry.tag_suffix_.rbegin()){
        if(!it->second.empty()){
            lower_view_id = it->first;
            tag_suffix_link_lower = &it->second;
            break;
        }
        ++it;
    }
    if (tag_suffix_link_lower == nullptr) {
        UNREACHABLE();
        return IndexQueryResult::kEmpty;
    }
    id = bits::JoinTwo16(lower_view_id, space_id);
    TagSuffixLinkGetTail(*tag_suffix_link_lower, id, seqnum, storage_shard_id);
    HVLOG_F(1, "FindPrev(query_tag={}, query_seqnum={}): Seqnum at lower link suffix tail -> found", user_tag, bits::HexStr0x(query_seqnum));
    return IndexQueryResult::kFound;
}

IndexQueryResult::State PerSpaceTagCache::FindNext(uint64_t query_seqnum, uint64_t user_tag, uint16_t space_id,
                                                   uint64_t* seqnum, uint16_t* storage_shard_id) const {
    HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Start", user_tag, bits::HexStr0x(query_seqnum));
    if (!tags_.contains(user_tag)) {
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Tag not in cache -> empty", user_tag, bits::HexStr0x(query_seqnum));
        return IndexQueryResult::kEmpty;
    }
    uint16_t view_id = log_utils::GetViewId(query_seqnum);
    TagEntry tag_entry = *tags_.at(user_tag).get();
    // check before min seqnum
    if (tag_entry.seqnum_min_ != kInvalidLogSeqNum && query_seqnum <= tag_entry.seqnum_min_){
        *seqnum = tag_entry.seqnum_min_;
        *storage_shard_id = tag_entry.shard_id_min_;
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): seqnum equal|lower than min -> found", user_tag, bits::HexStr0x(query_seqnum));
        return IndexQueryResult::kFound;
    }
    // check between min seqnum and suffix: gap
    if (TagSuffixGetHead(tag_entry.tag_suffix_, space_id, seqnum, storage_shard_id)){
        if (query_seqnum + 1 < *seqnum) {
            //gap
            HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Seqnum lies before head -> empty", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kEmpty;
        } else if (query_seqnum == *seqnum) {
            HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Seqnum lies on head -> found", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kFound;
        }
    } else {
        // if head is empty all is empty
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Head empty, all empty -> empty", user_tag, bits::HexStr0x(query_seqnum));
        return IndexQueryResult::kEmpty;
    }
    // check after tail
    if (TagSuffixGetTail(tag_entry.tag_suffix_, space_id, seqnum, storage_shard_id)){
        if (*seqnum == query_seqnum) {
            HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Query seqnum is at tail -> found", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kFound;
        }
        if (*seqnum < query_seqnum) {
            HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Query seqnum is after tail -> invalid", user_tag, bits::HexStr0x(query_seqnum));
            return IndexQueryResult::kInvalid;
        }
    }
    // invariant: seqnum lies after min seqnum and global tail
    uint32_t identifier;
    TagSuffixLink* tag_suffix_link = nullptr;
    auto it = tag_entry.tag_suffix_.lower_bound(view_id);
    // get lower
    while (it != tag_entry.tag_suffix_.end()){
        if (!it->second.empty()){
            view_id = it->first;
            tag_suffix_link = &it->second;
            ++it;
            break;
        }
        ++it;
    }
    if (tag_suffix_link == nullptr){
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): No seqnum entry for view equal|higher {} -> invalid", user_tag, bits::HexStr0x(query_seqnum), view_id);
        return IndexQueryResult::kInvalid;
    }
    identifier = bits::JoinTwo16(view_id, space_id);
    if(TagSuffixLinkFindNext(*tag_suffix_link, identifier, query_seqnum, seqnum, storage_shard_id)){
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Seqnum in lower link suffix -> found", user_tag, bits::HexStr0x(query_seqnum), view_id);
        return IndexQueryResult::kFound;
    }
    // get upper
    tag_suffix_link = nullptr;
    while (it != tag_entry.tag_suffix_.end()){
        if (!it->second.empty()){
            view_id = it->first;
            tag_suffix_link = &it->second;
            break;
        }
        ++it;
    }
    if(tag_suffix_link == nullptr) {
        // create invalid result because seqnum lies in the future
        HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Query seqnum in future -> invalid", user_tag, bits::HexStr0x(query_seqnum), view_id);
        return IndexQueryResult::kInvalid;
    }
    // get head of next suffix entry
    identifier = bits::JoinTwo16(view_id, space_id);
    TagSuffixLinkGetHead(*tag_suffix_link, identifier, seqnum, storage_shard_id);
    HVLOG_F(1, "FindNext(query_tag={}, query_seqnum={}): Seqnum at upper link suffix head -> found", user_tag, bits::HexStr0x(query_seqnum));
    return IndexQueryResult::kFound;
}

TagCacheView::TagCacheView(uint16_t view_id)
    : view_id_(view_id),
      metalog_position_(0)
    //   data_received_seqnum_position_(0)
    {
        log_header_ = fmt::format("TagCacheView[{}]: ", view_id);
    }

TagCacheView::~TagCacheView() {}

bool TagCacheView::CheckIfNewIndexData(const IndexDataProto& index_data){
    auto storage_shards = index_data.my_active_storage_shards();
    bool index_data_new = false;
    for(int i = 0; i < index_data.metalog_positions_size(); i++){
        uint32_t metalog_position = index_data.metalog_positions().at(i);
        size_t active_shards = index_data.num_active_storage_shards().at(i);
        if (metalog_position <= metalog_position_){
            continue;
        }
        if(storage_shards_index_updates_.contains(metalog_position)){
            size_t before_update = storage_shards_index_updates_.at(metalog_position).second.size();
            storage_shards_index_updates_.at(metalog_position).second.insert(storage_shards.begin(), storage_shards.end());
            size_t after_update = storage_shards_index_updates_.at(metalog_position).second.size();
            DCHECK_GE(after_update, before_update);
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
            index_data_new = true;
            if(!storage_shards_index_updates_.contains(metalog_position)){
                HVLOG(1) << "Super strange";
            }
            if(!storage_shards_index_updates_.contains(metalog_position_+1)){
                HVLOG(1) << "Super super strange";
            }
        }
    }
    return index_data_new;
}

bool TagCacheView::TryCompleteIndexUpdates(){
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
        storage_shards_index_updates_.erase(next_metalog_position);
        HVLOG_F(1, "Shards for metalog position {} completed", next_metalog_position);
        return true;
    }
    HVLOG_F(1, "Shards for metalog position {} not yet completed", next_metalog_position);
    return false;
}

TagCache::TagCache(uint16_t sequencer_id, size_t cache_size)
    : sequencer_id_(sequencer_id),
      cache_size_(cache_size)
    {
        log_header_ = fmt::format("TagCache[{}]: ", sequencer_id);
    }

TagCache::~TagCache() {}

void TagCache::ProvideIndexData(uint16_t view_id, const IndexDataProto& index_data){
    HVLOG(1) << "Provide index data";
    DCHECK_EQ(current_logspace_id(), index_data.logspace_id());
    if (!views_.at(view_id)->CheckIfNewIndexData(index_data)) {
        return;
    }
    // if (view.first_index_data_ && index_data.has_index_data()){
    //     HVLOG_F(1, "Initialize data_received_seqnum_position with {}", index_data.seqnum_halves().at(0));
    //     data_received_seqnum_position_ = index_data.seqnum_halves().at(0);
    //     view.first_index_data_ = false;
    // }
    HVLOG(1) << "Receive new index data";
    uint64_t popularity = bits::JoinTwo32(latest_view_id_, latest_metalog_position());
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
        // if (seqnum < seqnum_position_) {
        //     HVLOG_F(1, "Seqnum={} lower than IndexedSeqnumPosition={}", seqnum, seqnum_position_);
        //     tag_iter += num_tags;
        //     continue;
        // }
        for(size_t j = 0; j < num_tags; j++){
            uint32_t user_logspace = index_data.user_logspaces(i);
            GetOrCreatePerSpaceTagCache(user_logspace)->AddOrUpdate(
                *tag_iter, 
                view_id,
                sequencer_id_,
                seqnum, 
                gsl::narrow_cast<uint16_t>(index_data.engine_ids(i)),
                popularity
            );
            tags_list_.push_front({user_logspace, *tag_iter, popularity});            
        }
        // data_received_seqnum_position_ += num_tags;
        tag_iter += num_tags;
        // seqnum_position_ = seqnum;
    }
    Clear();
    // todo: blocking reads
    AdvanceIndexProgress(view_id);
}

void TagCache::ProvideMinSeqnumData(uint32_t user_logspace, uint64_t tag, const IndexResultProto& index_result_proto){
    if (index_result_proto.found()){
        DCHECK(index_result_proto.seqnum() != kInvalidLogSeqNum);
    } else {
        DCHECK(index_result_proto.seqnum() == kInvalidLogSeqNum);
    }
    GetOrCreatePerSpaceTagCache(user_logspace)->HandleMinSeqnum(
        tag,
        index_result_proto.seqnum(),
        gsl::narrow_cast<uint16_t>(index_result_proto.storage_shard_id())
    );
}

void TagCache::Clear(){
    while(tags_list_.size() > cache_size_){
        auto last_it = tags_list_.end(); last_it--;
        auto& [logspace, tag, popularity] = *last_it;
        per_space_cache_.at(logspace)->Remove(tag, popularity);
        tags_list_.pop_back();
    }   
}

bool TagCache::TagExists(uint32_t user_logspace, uint64_t tag){
    return GetOrCreatePerSpaceTagCache(user_logspace)->TagExists(tag);
}

void TagCache::InstallView(uint16_t view_id){
    DCHECK(!views_.contains(view_id));
    TagCacheView* tag_cache_view = new TagCacheView(view_id);
    views_[view_id].reset(tag_cache_view);
    latest_view_id_ = view_id;
}

bool TagCache::AdvanceIndexProgress(uint16_t view_id){
    bool advanced = false;
    while (views_.at(view_id)->TryCompleteIndexUpdates()){
        advanced = true;
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

// void TagCache::AdvanceIndexProgress() {
//     while (!cuts_.empty()) {
//         uint32_t end_seqnum = cuts_.front().second;
//         if (data_received_seqnum_position_ < end_seqnum) {
//             HVLOG_F(1, "DataReceivedSeqnumPosition={} is lower then endSeqnum={} of cut.", bits::HexStr0x(data_received_seqnum_position_), bits::HexStr0x(end_seqnum));
//             break;
//         }
//         HVLOG_F(1, "Apply IndexData until seqnum {}", bits::HexStr0x(end_seqnum));
//         auto iter = received_data_.begin();
//         while (iter != received_data_.end()) {
//             uint32_t seqnum = iter->first;
//             if (seqnum >= end_seqnum) {
//                 break;
//             }
//             const IndexData& index_data = iter->second;
//             GetOrCreateIndex(index_data.user_logspace)->Add(
//                 seqnum, index_data.engine_id, index_data.user_tags);
//             iter = received_data_.erase(iter);
//         }
//         DCHECK_GT(end_seqnum, indexed_seqnum_position_);
//         indexed_seqnum_position_ = end_seqnum;
//         uint32_t metalog_seqnum = cuts_.front().first;
//         indexed_metalog_position_ = metalog_seqnum + 1;
//         cuts_.pop_front();
//     }
//     // process pending queries
//     auto iter = pending_queries_.begin();
//     while (iter != pending_queries_.end()) {
//         if (iter->first > indexed_metalog_position_) {
//             break;
//         }
//         const IndexQuery& query = iter->second;
//         ProcessQuery(query);
//         iter = pending_queries_.erase(iter);
//     }
//     // delete unpopular data
//     // strategy: LRU without batching
//     while(tags_list_.size() > cache_size_){
//         auto last_it = tags_list_.end(); last_it--;
//         auto& [logspace, tag, popularity] = *last_it;
//         per_space_cache_.at(logspace)->Remove(tag, popularity);
//         tags_list_.pop_back();
//     }
// }

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
    if (!per_space_cache_.contains(query.user_logspace)){
        LOG(WARNING) << "Unknown logspace";
        pending_query_results_.push_back(BuildNotFoundResult(query));
        return;
    }
    uint64_t seqnum;
    uint16_t storage_shard_id;
    IndexQueryResult::State state;
    if (query.direction == IndexQuery::kReadNext) {
        state = GetOrCreatePerSpaceTagCache(query.user_logspace)->FindNext(
            query.query_seqnum,
            query.user_tag,
            sequencer_id_,
            &seqnum,
            &storage_shard_id
        );
    } else if (query.direction == IndexQuery::kReadPrev) {
        state = GetOrCreatePerSpaceTagCache(query.user_logspace)->FindPrev(
            query.query_seqnum,
            query.user_tag,
            sequencer_id_,
            &seqnum,
            &storage_shard_id
        );
    } else {
        state = IndexQueryResult::kInvalid;
        UNREACHABLE();
    }
    switch(state){
        case IndexQueryResult::kFound:
            GetOrCreatePerSpaceTagCache(query.user_logspace)->UpdatePopularity(query.user_tag, index_metalog_progress());
            Clear();
            pending_query_results_.push_back(BuildFoundResult(query, latest_view_id_, seqnum, storage_shard_id));
            break;
        case IndexQueryResult::kEmpty:
            pending_query_results_.push_back(BuildNotFoundResult(query));
            break;
        case IndexQueryResult::kInvalid:
            pending_query_results_.push_back(BuildInvalidResult(query));
            break;
        case IndexQueryResult::kContinue:
            LOG(FATAL) << "No need for kContinue";
            break;
    }
}

IndexQueryResult TagCache::BuildFoundResult(const IndexQuery& query, uint16_t view_id,
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

IndexQueryResult TagCache::BuildNotFoundResult(const IndexQuery& query) {
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

IndexQueryResult TagCache::BuildInvalidResult(const IndexQuery& query) {
    return IndexQueryResult {
        .state = IndexQueryResult::kInvalid,
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

}
}