#pragma once

#include "log/log_space_base.h"
#include "log/index.h"

namespace faas {
namespace log {

struct LinkEntryCompressed {
    LinkEntryCompressed(uint16_t highest_productive_shard, std::vector<std::pair<uint16_t, uint32_t>> productive_shards)
    : highest_shard_pos_(highest_productive_shard),
      progress_(std::vector<bool>(highest_productive_shard, false)),
      seqnum_upper_bounds_(absl::FixedArray<uint32_t>(productive_shards.size()-1, 0)) 
    {
        for(size_t i = 0; i < productive_shards.size() - 1; i++){ // - 1 because highest is ignored
            progress_.at(productive_shards.at(i).first) = true;
            seqnum_upper_bounds_.at(i) = productive_shards.at(i).second;
        }
    }
    LinkEntryCompressed(uint16_t highest_productive_shard)
    : highest_shard_pos_(highest_productive_shard),
      seqnum_upper_bounds_(absl::FixedArray<uint32_t>{})
    {}
    uint16_t highest_shard_pos_;
    std::vector<bool> progress_;
    absl::FixedArray<uint32_t> seqnum_upper_bounds_;
};

struct LinkEntry{
    LinkEntry(std::vector<std::pair<uint16_t, uint32_t>> productive_shards)
    : storage_shard_ids_(absl::FixedArray<uint16_t>(productive_shards.size(), 0)),
      key_diffs_(absl::FixedArray<uint8_t>(productive_shards.size()-1, 0))
    {
        DCHECK(0 < productive_shards.size());
        auto& [key_shard, key_seqnum] = productive_shards.back();
        for(size_t i = productive_shards.size() - 1; i > 0; i--) {
            auto& [shard, seqnum] = productive_shards.at(i);
            DCHECK(key_seqnum > seqnum);
            key_diffs_.at(i-1) = gsl::narrow_cast<uint8_t>(key_seqnum - seqnum);
            storage_shard_ids_.at(i) = shard;
        }
        storage_shard_ids_.at(0) = key_shard;
    }
    absl::FixedArray<uint16_t> storage_shard_ids_;
    absl::FixedArray<uint8_t> key_diffs_;
};

class SeqnumSuffixLink final : public LogSpaceBase {
public:
    SeqnumSuffixLink(const View* view, uint16_t sequencer_id);
    ~SeqnumSuffixLink();

    void Trim(uint64_t bound);
    void Trim(size_t* counter);
    bool IsEmpty();

    size_t NumEntries();
    void Aggregate(size_t* num_link_entries, size_t* num_range_entries, size_t* size);

    void GetHead(uint64_t* seqnum, uint16_t* storage_shard_id);
    void GetTail(uint64_t* seqnum, uint16_t* storage_shard_id);

    bool FindNext(uint64_t query_seqnum, uint64_t* seqnum, uint16_t* storage_shard_id);
    bool FindPrev(uint64_t query_seqnum, uint64_t* seqnum, uint16_t* storage_shard_id);

    uint64_t metalog_progress() const {
        return bits::JoinTwo32(identifier(), metalog_position());
    }

private:
    std::map<uint32_t, LinkEntry> entries_;

    bool first_metalog_;

    void OnNewLogs(std::vector<std::pair<uint16_t, uint32_t>> productive_cuts) override;
    
    void CreateEntry(const std::vector<uint32_t> productive_storage_shard_ids, const std::vector<uint32_t>& progresses);

    IndexQueryResult ProcessQuery(const IndexQuery& query);

    DISALLOW_COPY_AND_ASSIGN(SeqnumSuffixLink);
};

class SeqnumSuffixChain {
public:
    SeqnumSuffixChain(uint16_t sequence_number_id, size_t max_suffix_seq_entries, float trim_level);
    ~SeqnumSuffixChain();

    using QueryResultVec = absl::InlinedVector<IndexQueryResult, 4>;

    uint32_t identifier() const {
        return bits::JoinTwo16(0, sequence_number_id_);
    }

    // call only when chain is not empty
    uint64_t metalog_progress(){
        return (--suffix_chain_.end())->second->metalog_progress();
    }
    uint32_t metalog_position(){
        return (--suffix_chain_.end())->second->metalog_position();
    }
    uint16_t view_id(){
        return (--suffix_chain_.end())->second->view_id();
    }

    void Extend(const View* view);
    void Trim(uint64_t bound);
    void Trim(size_t* counter);
    //SuffixSeq* GetLast();
    void ProvideMetaLog(const MetaLogProto& metalog_proto);
    void MakeQuery(const IndexQuery& query);
    void PollQueryResults(QueryResultVec* results);
    void Aggregate(size_t* num_link_entries, size_t* num_range_entries, size_t* size);

private:
    uint16_t sequence_number_id_;
    size_t max_suffix_seq_entries_;
    std::string log_header_;
    float trim_level_;
    size_t current_entries_;
    std::map<uint16_t, std::unique_ptr<SeqnumSuffixLink>> suffix_chain_;

    std::multimap</* metalog_position */ uint32_t,
                  IndexQuery> pending_queries_;
    QueryResultVec pending_query_results_;

    bool IsEmpty();
    bool GetHead(uint64_t* head, uint16_t* storage_shard_id);
    bool GetTail(uint64_t* tail, uint16_t* storage_shard_id);
    bool GetHead(uint64_t* head);
    bool GetTail(uint64_t* tail);

    IndexQueryResult ProcessQuery(const IndexQuery& query);
    IndexQueryResult ProcessReadNext(const IndexQuery& query);
    IndexQueryResult ProcessReadPrev(const IndexQuery& query);

    IndexQueryResult BuildFoundResult(const IndexQuery& query, uint16_t view_id, 
                                      uint64_t seqnum, uint16_t storage_shard_id);
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query);
    IndexQueryResult BuildInvalidResult(const IndexQuery& query);

};

} // namespace log
} // namespace faas