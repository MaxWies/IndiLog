#pragma once

#include "log/log_space_base.h"
#include "log/index.h"

namespace faas {
namespace log {

struct LinkEntry {
    LinkEntry(uint16_t highest_productive_shard, std::vector<std::pair<uint16_t, uint32_t>> productive_shards)
    : highest_shard_pos_(highest_productive_shard),
      progress_(std::vector<bool>(highest_productive_shard, false)),
      seqnum_upper_bounds_(absl::FixedArray<uint32_t>(productive_shards.size()-1, 0)) 
    {
        for(size_t i = 0; i < productive_shards.size() - 1; i++){ // - 1 because highest is ignored
            progress_.at(productive_shards.at(i).first) = true;
            seqnum_upper_bounds_.at(i) = productive_shards.at(i).second;
        }
    }
    LinkEntry(uint16_t highest_productive_shard)
    : highest_shard_pos_(highest_productive_shard),
      seqnum_upper_bounds_(absl::FixedArray<uint32_t>{})
    {}
    uint16_t highest_shard_pos_;
    std::vector<bool> progress_;
    absl::FixedArray<uint32_t> seqnum_upper_bounds_;
};

class SeqnumSuffixLink final : public LogSpaceBase {
public:
    SeqnumSuffixLink(const View* view, uint16_t sequencer_id);
    ~SeqnumSuffixLink();

    void Trim(uint64_t bound);
    void Trim(size_t* counter);
    bool IsEmpty();

    size_t ComputeSize();
    size_t NumEntries();

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
    IndexQueryResult MakeQuery(const IndexQuery& query);
    size_t ComputeSize();

private:
    uint16_t sequence_number_id_;
    size_t max_suffix_seq_entries_;
    std::string log_header_;
    float trim_level_;
    size_t current_entries_;
    std::map<uint16_t, std::unique_ptr<SeqnumSuffixLink>> suffix_chain_;

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