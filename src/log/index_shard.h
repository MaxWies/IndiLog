#pragma once

#include "log/index.h"

namespace faas {
namespace log {

class IndexShard final : public Index {
    
public:

    IndexShard(const View* view, uint16_t sequencer_id, uint16_t index_shard_id, size_t num_shards);
    ~IndexShard();

    void ProvideIndexData(const IndexDataProto& index_data);
    bool AdvanceIndexProgress(const IndexDataProto& index_data);

private:

    absl::flat_hash_map<uint32_t /* metalog_position */, std::pair<size_t, absl::flat_hash_set<uint16_t>>> storage_shards_index_updates_;
    absl::flat_hash_map<uint32_t /* metalog_position */, uint32_t> end_seqnum_positions_;
    size_t num_shards_;

    bool TryCompleteIndexUpdates(uint32_t* seqnum_position);
    bool CheckIfNewIndexData(const IndexDataProto& index_data);
    uint64_t index_metalog_progress() const override;

    DISALLOW_COPY_AND_ASSIGN(IndexShard);
};

}  // namespace log
}  // namespace faas
