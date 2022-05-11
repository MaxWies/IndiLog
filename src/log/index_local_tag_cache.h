#pragma once

#include "log/index.h"

namespace faas {
namespace log {

using TagSuffixLink = std::map<uint32_t, uint16_t>;
using TagSuffix = std::map<uint16_t, TagSuffixLink>;

class TagEntry{

public:
    TagEntry(uint64_t seqnum_min, uint16_t storage_shard_id_min, uint64_t popularity, bool complete);
    TagEntry(TagSuffix tag_suffix, uint64_t popularity);
    TagEntry(uint16_t view_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity);
    ~TagEntry();

    void Add(uint16_t view_id, uint32_t seqnum, uint16_t storage_shard_id, uint64_t popularity);
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
    void HandleMinSeqnum(uint64_t tag, uint64_t min_seqnum, uint16_t min_storage_shard_id, uint16_t sequencer_id, uint64_t popularity);
    void Remove(uint64_t tag, uint64_t popularity);
    void Remove(uint64_t popularity);
    void UpdatePopularity(uint64_t tag, uint64_t popularity);
    bool TagExists(uint64_t tag);

    IndexQueryResult::State FindPrev(uint64_t query_seqnum, uint64_t user_tag, uint16_t space_id, uint64_t popularity,
                                     uint64_t* seqnum, uint16_t* engine_id) const;
    IndexQueryResult::State FindNext(uint64_t query_seqnum, uint64_t user_tag, uint16_t space_id, uint64_t popularity,
                                     uint64_t* seqnum, uint16_t* engine_id) const;

private:
    uint32_t user_logspace_;
    std::string log_header_;
    // tag-key : tag-entry
    absl::flat_hash_map<uint64_t, std::unique_ptr<TagEntry>> tags_;
    absl::flat_hash_set<uint64_t> pending_seqnum_min_;

    bool TagEntryExists(uint64_t key);
};

class TagCacheView {
public:
    TagCacheView(uint16_t view_id);
    ~TagCacheView();

    bool CheckIfNewIndexData(const IndexDataProto& index_data);
    bool TryCompleteIndexUpdates();
    uint32_t metalog_position(){
        return metalog_position_;
    }

private:
    uint16_t view_id_;
    std::string log_header_;
    uint32_t metalog_position_;
    absl::flat_hash_map<uint32_t /* metalog_position */, std::pair<size_t, absl::flat_hash_set<uint16_t>>> storage_shards_index_updates_;
};

class TagCache {
public:
    static constexpr absl::Duration kBlockingQueryTimeout = absl::Seconds(1);

    TagCache(uint16_t sequencer_id_, size_t cache_size);
    ~TagCache();

    using QueryResultVec = absl::InlinedVector<IndexQueryResult, 4>;

    void ProvideIndexData(uint16_t view_id, const IndexDataProto& index_data_proto);
    void ProvideMinSeqnumData(uint32_t user_logspace, uint64_t tag, const IndexResultProto& index_result_proto);
    bool AdvanceIndexProgress(uint16_t view_id);
    void MakeQuery(const IndexQuery& query);
    void PollQueryResults(QueryResultVec* results);
    bool TagExists(uint32_t user_logspace, uint64_t tag);
    void InstallView(uint16_t view_id);

    uint32_t identifier(){
        return bits::JoinTwo16(0, sequencer_id_);
    }

    // bool Finalize(uint32_t final_metalog_position, const std::vector<MetaLogProto>& tail_metalogs);

private:
    uint16_t sequencer_id_;

    uint16_t latest_view_id_;
    absl::flat_hash_map<uint16_t, std::unique_ptr<TagCacheView>> views_;

    std::string log_header_;

    // uint32_t seqnum_position_;
    // uint32_t metalog_position_;

    std::multimap</* metalog_position */ uint32_t,
                  IndexQuery> pending_queries_;
    QueryResultVec pending_query_results_;

    size_t cache_size_;

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
    std::list<std::tuple<uint32_t, uint64_t, uint64_t>> tags_list_;

    PerSpaceTagCache* GetOrCreatePerSpaceTagCache(uint32_t user_logspace);

    // void OnFinalized(uint32_t metalog_position);
    void Trim(size_t* counter);
    void Clear();

    void ProcessQuery(const IndexQuery& query);
    bool ProcessBlockingQuery(const IndexQuery& query);

    IndexQueryResult BuildFoundResult(const IndexQuery& query, uint16_t view_id,
                                      uint64_t seqnum, uint16_t engine_id);
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query);
    IndexQueryResult BuildInvalidResult(const IndexQuery& query);
    IndexQueryResult BuildContinueResult(const IndexQuery& query, bool found,
                                         uint64_t seqnum, uint16_t engine_id);

};


}
}