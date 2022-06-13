#pragma once

#include "log/index.h"

namespace faas {
namespace log {

struct PendingMinTag {
public:
    uint64_t tag;
    uint64_t seqnum;
    uint16_t storage_shard_id;
    uint32_t user_logspace;
    uint64_t timestamp;
};

class TagSuffixLink {
public:
    TagSuffixLink(uint32_t seqnum, uint16_t storage_shard_id);
    bool FindPrev(uint32_t identifier, uint64_t query_seqnum, uint64_t* seqnum, uint16_t* shard_id) const;
    void GetTail(uint32_t identifier, uint64_t* seqnum, uint16_t* shard_id) const;
    bool FindNext(uint32_t identifier, uint64_t query_seqnum, uint64_t* seqnum, uint16_t* shard_id) const;
    void GetHead(uint32_t identifier, uint64_t* seqnum, uint16_t* shard_id) const;
    std::vector<uint32_t> seqnums_;
    std::vector<uint16_t> storage_shard_ids_;
private:
};

using TagSuffix = std::map<uint16_t, std::unique_ptr<TagSuffixLink>>;

class TagEntry{

public:
    TagEntry(uint64_t seqnum_min, uint16_t storage_shard_id_min, uint64_t popularity, bool complete);
    TagEntry(TagSuffix tag_suffix, uint64_t popularity);
    TagEntry(uint16_t view_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity);
    ~TagEntry();

    void Add(uint16_t view_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity);
    void Evict(uint32_t per_tag_seqnums_limit, size_t* num_evicted_seqnums);
    void GetSuffixHead(uint16_t sequencer_id, uint64_t* seqnum, uint16_t* shard_id) const;
    void GetSuffixTail(uint16_t sequencer_id, uint64_t* seqnum, uint16_t* shard_id) const;
    size_t NumSeqnumsInSuffix();

    TagSuffix tag_suffix_;
    uint64_t seqnum_min_;
    uint16_t shard_id_min_;
    uint64_t popularity_;
    bool complete_;
private:
};



class PerSpaceTagCache {
public:
    PerSpaceTagCache(uint32_t user_logspace);
    ~PerSpaceTagCache();

    void AddOrUpdate(uint64_t tag, uint16_t view_id, uint16_t sequencer_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity);
    void HandleMinSeqnum(uint64_t tag, uint64_t min_seqnum, uint16_t min_storage_shard_id, uint64_t timestamp, uint16_t sequencer_id, uint64_t popularity);
    void Remove(uint64_t tag, uint64_t popularity);
    void Remove(uint64_t popularity);
    void Evict(uint64_t popularity, uint32_t per_tag_seqnums_limit, size_t* evicted_seqnums);
    void Clear();
    bool TagExists(uint64_t tag);
    void Aggregate(size_t* num_tags, size_t* num_seqnums, size_t* size);

    IndexQueryResult::State FindPrev(uint64_t query_seqnum, uint64_t user_tag, uint16_t space_id, uint64_t popularity,
                                     uint64_t* seqnum, uint16_t* engine_id);
    IndexQueryResult::State FindNext(uint64_t query_seqnum, uint64_t user_tag, uint16_t space_id, uint64_t popularity,
                                     uint64_t* seqnum, uint16_t* engine_id);

private:
    uint32_t user_logspace_;
    std::string log_header_;
    // tag-key : tag-entry
    absl::flat_hash_map<uint64_t, std::unique_ptr<TagEntry>> tags_;
    absl::flat_hash_map<uint64_t, std::unique_ptr<TagEntry>> pending_min_tags_;

    bool TagEntryExists(uint64_t key);
};

class TagCacheView {
public:
    TagCacheView(uint16_t view_id, uint32_t metalog_position);
    ~TagCacheView();

    bool CheckIfNewIndexData(const IndexDataProto& index_data);
    bool TryCompleteIndexUpdates(uint32_t* end_seqnum_position);
    uint32_t metalog_position(){
        return metalog_position_;
    }

private:
    uint16_t view_id_;
    std::string log_header_;
    uint32_t metalog_position_;
    bool first_metalog_;
    absl::flat_hash_map<uint32_t /* metalog_position */, std::pair<size_t, absl::flat_hash_set<uint16_t>>> storage_shards_index_updates_;
    absl::flat_hash_map<uint32_t /* metalog_position */, uint32_t> end_seqnum_positions_;
};

class TagCache {
public:
    static constexpr absl::Duration kBlockingQueryTimeout = absl::Seconds(1);

    TagCache(uint16_t sequencer_id_, size_t max_cache_size, uint32_t per_tag_seqnums_limit);
    ~TagCache();

    using QueryResultVec = absl::InlinedVector<IndexQueryResult, 4>;

    void ProvideIndexData(uint16_t view_id, const IndexDataProto& index_data_proto, std::vector<PendingMinTag>& pending_min_tags);
    // void ProvideMinSeqnumData(uint32_t user_logspace, uint64_t tag, const IndexResultProto& index_result_proto);
    bool AdvanceIndexProgress(uint16_t view_id);
    void MakeQuery(const IndexQuery& query);
    void PollQueryResults(QueryResultVec* results);
    bool TagExists(uint32_t user_logspace, uint64_t tag);
    void InstallView(uint16_t view_id, uint32_t metalog_position);
    void Clear();
    void Aggregate(size_t* num_tags, size_t* num_seqnums, size_t* size);

    uint32_t identifier(){
        return bits::JoinTwo16(0, sequencer_id_);
    }

    // bool Finalize(uint32_t final_metalog_position, const std::vector<MetaLogProto>& tail_metalogs);

private:
    uint16_t sequencer_id_;

    uint16_t latest_view_id_;
    absl::flat_hash_map<uint16_t, std::unique_ptr<TagCacheView>> views_;

    std::string log_header_;

    struct IndexData {
        uint16_t   engine_id;
        uint32_t   user_logspace;
        UserTagVec user_tags;
    };
    std::map</* seqnum */ uint32_t, IndexData> received_data_;

    std::multimap</* metalog_position */ uint32_t,
                  IndexQuery> pending_queries_;
    QueryResultVec pending_query_results_;

    size_t max_cache_size_;
    uint32_t per_tag_seqnums_limit_;
    size_t cache_size_;

    std::vector<uint64_t> popularity_sequence_;

    static constexpr uint32_t kMaxMetalogPosition = std::numeric_limits<uint32_t>::max();

    uint32_t current_logspace_id(){
        return bits::JoinTwo16(latest_view_id_, sequencer_id_);
    }
    uint32_t latest_metalog_position(){
        return views_.at(latest_view_id_)->metalog_position();
    }
    uint64_t index_metalog_progress(){
        return bits::JoinTwo32(identifier(), views_.at(latest_view_id_)->metalog_position());
    }

    absl::flat_hash_map</* user_logspace */ uint32_t, std::unique_ptr<PerSpaceTagCache>> per_space_cache_;

    PerSpaceTagCache* GetOrCreatePerSpaceTagCache(uint32_t user_logspace);

    // void OnFinalized(uint32_t metalog_position);
    void Trim(size_t* counter);
    void Evict();

    void ProcessQuery(const IndexQuery& query);
    bool ProcessBlockingQuery(const IndexQuery& query);

    IndexQueryResult BuildFoundResult(const IndexQuery& query, uint64_t metalog_progress, uint16_t view_id,
                                      uint64_t seqnum, uint16_t engine_id);
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query, uint64_t metalog_progress);
    IndexQueryResult BuildInvalidResult(const IndexQuery& query, uint64_t metalog_progress);

};


}
}