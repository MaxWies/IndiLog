#include "log/index_shard.h"

namespace faas {
namespace log {

IndexShard::IndexShard(const View* view, uint16_t sequencer_id, uint16_t index_shard_id, size_t num_shards)
    : Index(view, sequencer_id),
      num_shards_(num_shards) {
    indexed_metalog_position_ = uint32_t(index_shard_id);
}

IndexShard::~IndexShard() {}

void IndexShard::ProvideIndexData(const IndexDataProto& index_data) {
    int n = index_data.seqnum_halves_size();
    DCHECK_EQ(n, index_data.engine_ids_size());
    DCHECK_EQ(n, index_data.user_logspaces_size());
    DCHECK_EQ(n, index_data.user_tag_sizes_size());
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
        }
        tag_iter += num_tags;
    }
}

bool IndexShard::AdvanceIndexProgress(const IndexDataProto& index_data) {
    if(CheckIfNewIndexData(index_data)){
        ProvideIndexData(index_data);
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
                GetOrCreateIndex(index_data.user_logspace)->Add(
                    seqnum, index_data.engine_id, index_data.user_tags);
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
                        pending_query_results_.push_back(BuildEmptyResult(query));
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

bool IndexShard::CheckIfNewIndexData(const IndexDataProto& index_data){
    // careful with metalog position of meta header
    bool index_data_new = false;
    if (index_data.metalog_position() <= indexed_metalog_position_){
        HVLOG_F(1, "Received metalog_position={} lower|equal my metalog_position={}", index_data.metalog_position(), indexed_metalog_position_);
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

bool IndexShard::TryCompleteIndexUpdates(uint32_t* end_seqnum_position){
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
        // updates from all active storage shards received -> jump to next metalog based on number of shards
        indexed_metalog_position_ = indexed_metalog_position_ + uint32_t(num_shards_);
        *end_seqnum_position = end_seqnum_positions_.at(next_index_metalog_position);
        storage_shards_index_updates_.erase(next_index_metalog_position);
        end_seqnum_positions_.erase(next_index_metalog_position);
        HVLOG_F(1, "Shards for metalog position {} completed", next_index_metalog_position);
        return true;
    }
    HVLOG_F(1, "Shards for metalog position {} not yet completed", next_index_metalog_position);
    return false;
}

uint64_t IndexShard::index_metalog_progress() const {
    uint32_t real_index_metalog_progress = indexed_metalog_position_;
    uint32_t num_other_shards = 0;
    if (0 < num_shards_) {
        num_other_shards = uint32_t(num_shards_) - 1;
    }
    if (real_index_metalog_progress <= num_other_shards) {
        real_index_metalog_progress = 0;
    } else {
        real_index_metalog_progress -= num_other_shards;
    }
    return bits::JoinTwo32(identifier(), real_index_metalog_progress);
}

}  // namespace log
}  // namespace faas
