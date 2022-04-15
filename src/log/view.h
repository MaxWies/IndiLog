#pragma once

#include "base/common.h"
#include "utils/hash.h"
#include "utils/bits.h"
#include "utils/io.h"

__BEGIN_THIRD_PARTY_HEADERS
#include "proto/shared_log.pb.h"
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace log {

// View and its inner class will never change after construction
class View {
public:
    explicit View(const ViewProto& view_proto);
    ~View() {}

    uint16_t id() const { return id_; }

    size_t metalog_replicas() const { return metalog_replicas_; }
    size_t userlog_replicas() const { return userlog_replicas_; }
    size_t index_replicas() const { return index_replicas_; }
    size_t num_index_shards() const { return num_index_shards_; }
    size_t num_phylogs() const { return num_phylogs_; }

    size_t num_engine_nodes() const { return engine_node_ids_.size(); }
    size_t num_sequencer_nodes() const { return sequencer_node_ids_.size(); }
    size_t num_storage_nodes() const { return storage_node_ids_.size(); }
    size_t num_index_nodes() const { return index_node_ids_.size(); }

    using NodeIdVec = absl::FixedArray<uint16_t>;
    const NodeIdVec& GetEngineNodes() const { return engine_node_ids_; }
    const NodeIdVec& GetSequencerNodes() const { return sequencer_node_ids_; }
    const NodeIdVec& GetStorageNodes() const { return storage_node_ids_; }
    const NodeIdVec& GetIndexNodes() const { return index_node_ids_; }

    bool contains_engine_node(uint16_t node_id) const {
        return engine_nodes_.contains(node_id);
    }
    bool contains_sequencer_node(uint16_t node_id) const {
        return sequencer_nodes_.contains(node_id);
    }
    bool contains_storage_node(uint16_t node_id) const {
        return storage_nodes_.contains(node_id);
    }
    bool contains_index_node(uint16_t node_id) const {
        return index_nodes_.contains(node_id);
    }
    bool is_active_phylog(uint16_t sequencer_node_id) const {
        return active_phylogs_.contains(sequencer_node_id);
    }

    uint32_t LogSpaceIdentifier(uint32_t user_logspace) const {
        uint64_t h = hash::xxHash64(user_logspace, /* seed= */ log_space_hash_seed_);
        uint16_t node_id = log_space_hash_tokens_[h % log_space_hash_tokens_.size()];
        DCHECK(sequencer_nodes_.contains(node_id));
        return bits::JoinTwo16(id_, node_id);
    }

    uint64_t log_space_hash_seed() const { return log_space_hash_seed_; }
    const NodeIdVec& log_space_hash_tokens() const { return log_space_hash_tokens_; }

    class Engine {
    public:
        Engine(Engine&& other) = default;
        ~Engine() {}

        const View* view() const { return view_; }
        uint16_t node_id() const { return node_id_; }

        const View::NodeIdVec& GetStorageNodes() const {
            return storage_nodes_;
        }

        uint16_t PickStorageNode() const {
            size_t idx = __atomic_fetch_add(&next_storage_node_, 1, __ATOMIC_RELAXED);
            return storage_nodes_.at(idx % storage_nodes_.size());
        }

        size_t PickIndexShard() const {
            size_t sx = __atomic_fetch_add(&next_index_shard, 1, __ATOMIC_RELAXED);
            return sx % view_->num_index_shards_;
        }

        uint16_t PickIndexNode(size_t shard) const {
            View::NodeIdVec index_nodes = index_shard_nodes_.at(shard);
            size_t next_index_node = next_index_replica_node_.at(shard);
            size_t idx = __atomic_fetch_add(&next_index_node, 1, __ATOMIC_RELAXED);
            return index_nodes.at(idx % index_nodes.size());
        }

        void PickIndexNodePerShard(std::vector<uint16_t>& sharded_index_nodes) const {
            size_t first_shard = PickIndexShard(); // node of shard is master
            LOG_F(INFO, "Master shard is {} of {} shards", first_shard, view_->num_index_shards_);
            for (size_t i = first_shard; i < view_->num_index_shards_ + first_shard ; i++) {
                size_t j = i % view_->num_index_shards_;
                DCHECK_LE(j, static_cast<size_t>(view_->num_index_shards_ - 1));
                uint16_t index_node = PickIndexNode(j);
                LOG_F(INFO, "Pick index node {} of shard {}", index_node, j);
                sharded_index_nodes.push_back(index_node);
            } 
        }

        bool HasIndexFor(uint16_t sequencer_node_id) const {
            return indexed_sequencer_node_set_.contains(sequencer_node_id);
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        View::NodeIdVec storage_nodes_;
        absl::flat_hash_set<uint16_t> indexed_sequencer_node_set_;

        std::vector<size_t> next_index_replica_node_;
        std::vector<View::NodeIdVec> index_shard_nodes_;

        mutable size_t next_index_shard;
        mutable size_t next_storage_node_;

        Engine(const View* view, uint16_t node_id,
               const View::NodeIdVec& storage_nodes,
               const View::NodeIdVec& index_sequencer_nodes,
               const std::vector<View::NodeIdVec>& index_shard_nodes);
        DISALLOW_IMPLICIT_CONSTRUCTORS(Engine);
    };

    const Engine* GetEngineNode(uint16_t node_id) const {
        DCHECK(engine_nodes_.contains(node_id));
        return engine_nodes_.at(node_id);
    }

    class Sequencer {
    public:
        Sequencer(Sequencer&& other) = default;
        ~Sequencer() {}

        const View* view() const { return view_; }
        uint16_t node_id() const { return node_id_; }

        const View::NodeIdVec& GetReplicaSequencerNodes() const {
            return replica_sequencer_nodes_;
        }

        bool IsReplicaSequencerNode(uint16_t sequencer_node_id) const {
            return replica_sequencer_node_set_.contains(sequencer_node_id);
        }

        const View::NodeIdVec& GetIndexEngineNodes() const {
            return index_engine_nodes_;
        }

        bool IsIndexEngineNode(uint16_t engine_node_id) const {
            return index_engine_node_set_.contains(engine_node_id);
        }

        uint16_t PickIndexEngineNode() const {
            size_t idx = __atomic_fetch_add(&next_index_engine_node_, 1, __ATOMIC_RELAXED);
            return index_engine_nodes_.at(idx % index_engine_nodes_.size());
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        View::NodeIdVec replica_sequencer_nodes_;
        View::NodeIdVec index_engine_nodes_;
        absl::flat_hash_set<uint16_t> replica_sequencer_node_set_;
        absl::flat_hash_set<uint16_t> index_engine_node_set_;

        mutable size_t next_index_engine_node_;

        Sequencer(const View* view, uint16_t node_id,
                  const View::NodeIdVec& replica_sequencer_nodes,
                  const View::NodeIdVec& index_engine_nodes);
        DISALLOW_IMPLICIT_CONSTRUCTORS(Sequencer);
    };

    const Sequencer* GetSequencerNode(uint16_t node_id) const {
        DCHECK(sequencer_nodes_.contains(node_id));
        return sequencer_nodes_.at(node_id);
    }

    class Storage {
    public:
        Storage(Storage&& other) = default;
        ~Storage() {}

        const View* view() const { return view_; }
        uint16_t node_id() const { return node_id_; }

        const View::NodeIdVec& GetSourceEngineNodes() const {
            return source_engine_nodes_;
        }

        bool IsSourceEngineNode(uint16_t engine_node_id) const {
            return source_engine_node_set_.contains(engine_node_id);
        }

        // bool IsIndexDataSenderThisRound(uint16_t node_id) const {
        //     size_t idx = __atomic_fetch_add(&next_index_data_sender, 1, __ATOMIC_RELAXED);
        //     size_t j = idx % view_->index_replicas_;
        //     for(size_t i = 0; i < view_->storage_node_ids_.size(); i++){
        //         size_t u = i % view_->index_replicas_;

        //     }
        //     for (uint16_t storage_node : view_->storage_node_ids_){
                
        //     }
        // }

        // const View::NodeIdVec& GetIndexShardNodes(size_t shard) const {
        //     return index_shard_nodes_.at(shard);
        // }

        const View::NodeIdVec& PickIndexShardNodes() const {
            size_t idx = __atomic_fetch_add(&next_index_shard_, 1, __ATOMIC_RELAXED);
            return index_shard_nodes_.at(idx % view_->num_index_shards());
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        View::NodeIdVec source_engine_nodes_;
        absl::flat_hash_set<uint16_t> source_engine_node_set_;

        std::vector<View::NodeIdVec> index_shard_nodes_;
        mutable size_t next_index_shard_;
        mutable size_t next_index_data_sender;

        Storage(const View* view, uint16_t node_id,
                const View::NodeIdVec& source_engine_nodes,
                const std::vector<View::NodeIdVec>& index_shard_nodes);
        DISALLOW_IMPLICIT_CONSTRUCTORS(Storage);
    };

    const Storage* GetStorageNode(uint16_t node_id) const {
        DCHECK(storage_nodes_.contains(node_id));
        return storage_nodes_.at(node_id);
    }

    class Index {
    public:
        Index(Index&& other) = default;
        ~Index() {}

        const View* view() const { return view_; }
        uint16_t node_id() const { return node_id_; }

        // const View::NodeIdVec& GetStorageNodes(uint16_t engine_node) const {
        //     return engine_storage_nodes_.at(engine_node);
        // }

        uint16_t PickStorageNode(uint16_t engine_node) const {
            size_t next_storage_node_ = next_engine_storage_node_.at(engine_node);
            size_t idx = __atomic_fetch_add(&next_storage_node_, 1, __ATOMIC_RELAXED);
            size_t storage_node_pos = idx % view_->userlog_replicas_;
            LOG_F(INFO, "Use storage node at position {}", storage_node_pos);
            std::vector<uint16_t> engine_storage_nodes = engine_storage_nodes_.at(engine_node);
            DCHECK_EQ(view_->userlog_replicas_, engine_storage_nodes.size());
            return engine_storage_nodes.at(storage_node_pos);
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        //TODO: should be NodeIdVec
        absl::flat_hash_map<uint16_t, std::vector<uint16_t>> engine_storage_nodes_;
        absl::flat_hash_map<uint16_t, size_t> next_engine_storage_node_;

        Index(const View* view, uint16_t node_id,
              const absl::flat_hash_map<uint16_t, std::vector<uint16_t>>& engine_storage_nodes);
        DISALLOW_IMPLICIT_CONSTRUCTORS(Index);
    };

    const Index* GetIndexNode(uint16_t node_id) const {
        DCHECK(index_nodes_.contains(node_id));
        return index_nodes_.at(node_id);
    }

private:
    uint16_t id_;

    size_t metalog_replicas_;
    size_t userlog_replicas_;
    size_t index_replicas_;
    size_t num_index_shards_;
    size_t num_phylogs_;

    NodeIdVec engine_node_ids_;
    NodeIdVec sequencer_node_ids_;
    NodeIdVec storage_node_ids_;
    NodeIdVec index_node_ids_;

    absl::flat_hash_set<uint16_t> active_phylogs_;

    absl::InlinedVector<Engine, 16>    engines_;
    absl::InlinedVector<Sequencer, 16> sequencers_;
    absl::InlinedVector<Storage, 16>   storages_;
    absl::InlinedVector<Index, 16>     indexes_;

    absl::flat_hash_map<uint16_t, Engine*>    engine_nodes_;
    absl::flat_hash_map<uint16_t, Sequencer*> sequencer_nodes_;
    absl::flat_hash_map<uint16_t, Storage*>   storage_nodes_;
    absl::flat_hash_map<uint16_t, Index*>     index_nodes_;

    uint64_t  log_space_hash_seed_;
    NodeIdVec log_space_hash_tokens_;

    DISALLOW_COPY_AND_ASSIGN(View);
};

}  // namespace log
}  // namespace faas
