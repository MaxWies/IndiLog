#pragma once

#include "log/log_space_base.h"

namespace faas {
namespace log {

struct IndexFoundResult {
    uint16_t view_id;
    uint16_t storage_shard_id;
    uint64_t seqnum;
};

struct IndexQuery {
    enum ReadDirection { kReadNext, kReadPrev, kReadNextB };
    ReadDirection direction;
    uint16_t origin_node_id;
    uint16_t hop_times;
    bool     initial;
    uint64_t client_data;

    uint32_t user_logspace;
    uint64_t user_tag;
    uint64_t query_seqnum;
    uint64_t metalog_progress;

    uint16_t master_node_id;

    // todo: refactor, only for min queries in index tier
    bool min_seqnum_query;
    uint64_t tail_seqnum;

    IndexFoundResult prev_found_result;

    static ReadDirection DirectionFromOpType(protocol::SharedLogOpType op_type);
    protocol::SharedLogOpType DirectionToOpType() const;
    protocol::SharedLogOpType DirectionToIndexResult() const;
    std::string DirectionToString() const;
};

struct IndexQueryResult {
    enum State { kFound, kEmpty, kContinue, kInvalid };
    State state;
    uint64_t metalog_progress;
    uint16_t next_view_id;

    IndexQuery       original_query;
    IndexFoundResult found_result;

    uint32_t StorageShardId() const;
    bool IsFound() const;
    bool IsPointHit() const;
};

class Index final : public LogSpaceBase {
public:
    static constexpr absl::Duration kBlockingQueryTimeout = absl::Seconds(1);

    Index(const View* view, uint16_t sequencer_id);
    ~Index();

    void ProvideIndexData(const IndexDataProto& index_data);
    void ProvideIndexData(const IndexDataProto& index_data, uint16_t my_index_node_id);

    void MakeQuery(const IndexQuery& query);

    using QueryResultVec = absl::InlinedVector<IndexQueryResult, 4>;
    void PollQueryResults(QueryResultVec* results);

    void AddCut(uint32_t metalog_seqnum, uint32_t next_seqnum);

    void AdvanceIndexProgress();
    bool AdvanceIndexProgress(const IndexDataProto& index_data, uint16_t my_index_node_id);

    bool TryCompleteIndexUpdates(uint32_t* seqnum_position);
    bool CheckIfNewIndexData(const IndexDataProto& index_data);

    void Aggregate(size_t* num_seqnums, size_t* num_tags, size_t* num_seqnums_of_tags, size_t* size);

    uint32_t indexed_metalog_position(){
        return indexed_metalog_position_;
    }

private:
    class PerSpaceIndex;
    absl::flat_hash_map</* user_logspace */ uint32_t,
                        std::unique_ptr<PerSpaceIndex>> index_;

    static constexpr uint32_t kMaxMetalogPosition = std::numeric_limits<uint32_t>::max();

    std::multimap</* metalog_position */ uint32_t,
                  IndexQuery> pending_queries_;
    std::vector<std::pair</* start_timestamp */ int64_t,
                          IndexQuery>> blocking_reads_;
    QueryResultVec pending_query_results_;

    std::deque<std::pair</* metalog_seqnum */ uint32_t,
                         /* end_seqnum */ uint32_t>> cuts_;
    uint32_t indexed_metalog_position_;

    bool first_index_data_; // for local indexing

    // for index tier
    absl::flat_hash_map<uint32_t /* metalog_position */, std::pair<size_t, absl::flat_hash_set<uint16_t>>> storage_shards_index_updates_;
    absl::flat_hash_map<uint32_t /* metalog_position */, uint32_t> end_seqnum_positions_;

    struct IndexData {
        uint16_t   engine_id;
        uint32_t   user_logspace;
        UserTagVec user_tags;
        bool skip;
    };
    std::map</* seqnum */ uint32_t, IndexData> received_data_;
    uint32_t data_received_seqnum_position_;
    uint32_t indexed_seqnum_position_;

    uint64_t index_metalog_progress() const {
        return bits::JoinTwo32(identifier(), indexed_metalog_position_);
    }

    void OnMetaLogApplied(const MetaLogProto& meta_log_proto) override;
    void OnFinalized(uint32_t metalog_position) override;
    PerSpaceIndex* GetOrCreateIndex(uint32_t user_logspace);
    void TryCreateIndex(uint32_t user_logspace);

    void ProcessQuery(const IndexQuery& query);
    void ProcessReadNext(const IndexQuery& query);
    void ProcessReadPrev(const IndexQuery& query);
    bool ProcessBlockingQuery(const IndexQuery& query);

    bool IndexFindNext(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);
    bool IndexFindPrev(const IndexQuery& query, uint64_t* seqnum, uint16_t* engine_id);

    IndexQueryResult BuildFoundResult(const IndexQuery& query, uint16_t view_id,
                                      uint64_t seqnum, uint16_t engine_id);
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query);
    IndexQueryResult BuildContinueResult(const IndexQuery& query, bool found,
                                         uint64_t seqnum, uint16_t engine_id);

    DISALLOW_COPY_AND_ASSIGN(Index);
};

}  // namespace log
}  // namespace faas
