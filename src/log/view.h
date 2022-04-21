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

    size_t num_sequencer_nodes() const { return sequencer_node_ids_.size(); }
    size_t num_storage_nodes() const { return storage_node_ids_.size(); }
    size_t num_index_nodes() const { return index_node_ids_.size(); }
    size_t num_local_storage_shards() const { return local_storage_shard_ids_.size(); }
    size_t num_global_storage_shards() const { return global_storage_shard_ids_.size(); }

    using NodeIdVec = absl::FixedArray<uint16_t>;
    using ShardIdVec = absl::FixedArray<uint32_t>;

    const NodeIdVec& GetSequencerNodes() const { return sequencer_node_ids_; }
    const NodeIdVec& GetStorageNodes() const { return storage_node_ids_; }
    const NodeIdVec& GetIndexNodes() const { return index_node_ids_; }
    const NodeIdVec& GetLocalStorageShardIds() const {return local_storage_shard_ids_;}
    const ShardIdVec& GetGlobalStorageShardIds() const {return global_storage_shard_ids_;}

    const NodeIdVec& GetStorageShardIds(uint16_t sequencer_node_id) const {
        DCHECK(sequencer_storage_shard_ids_.contains(sequencer_node_id));
        return sequencer_storage_shard_ids_.at(sequencer_node_id);
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
    bool contains_storage_shard_id(uint16_t sequencer_id, uint16_t local_storage_shard_id) const {
        return storage_shard_units_.contains(bits::JoinTwo16(sequencer_id, local_storage_shard_id));
    }
    bool is_active_phylog(uint16_t sequencer_node_id) const {
        return active_phylogs_.contains(sequencer_node_id);
    }

    void GetActiveSequencerNodes(std::vector<uint16_t>* active_sequencers) const {
        for(uint16_t sequencer_node_id : GetSequencerNodes()){
            if(is_active_phylog(sequencer_node_id)){
                active_sequencers->push_back(sequencer_node_id);
            }
        }
    }

    uint32_t LogSpaceIdentifier(uint32_t user_logspace) const {
        uint64_t h = hash::xxHash64(user_logspace, /* seed= */ log_space_hash_seed_);
        uint16_t node_id = log_space_hash_tokens_[h % log_space_hash_tokens_.size()];
        DCHECK(sequencer_nodes_.contains(node_id));
        return bits::JoinTwo16(id_, node_id);
    }

    uint64_t log_space_hash_seed() const { return log_space_hash_seed_; }
    const NodeIdVec& log_space_hash_tokens() const { return log_space_hash_tokens_; }

    class StorageShard {
    public:
        StorageShard(StorageShard&& other) = default;
        ~StorageShard() {}

        uint32_t shard_id() const { return shard_id_; }

        uint16_t GetSequencerNode() const {
            return sequencer_node_;
        }

        const View::NodeIdVec& GetStorageNodes() const {
            return storage_nodes_;
        }

        bool HasStorageNode(uint16_t storage_node) const {
            for(uint16_t s : storage_nodes_){
                if(s == storage_node){
                    return true;
                }
            }
            return false;
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

    private:
        friend class View;
        const View* view_;
        uint32_t shard_id_; /*sequencer_id||storage_shard_id*/

        View::NodeIdVec storage_nodes_;
        uint16_t sequencer_node_;

        std::vector<size_t> next_index_replica_node_;
        std::vector<View::NodeIdVec> index_shard_nodes_;

        mutable size_t next_index_shard;
        mutable size_t next_storage_node_;

        StorageShard(const View* view, uint32_t shard_id,
               const View::NodeIdVec& storage_nodes,
               const uint16_t sequencer_node,
               const std::vector<View::NodeIdVec>& index_shard_nodes);
        DISALLOW_IMPLICIT_CONSTRUCTORS(StorageShard);
    };

    const StorageShard* GetStorageShard(uint32_t shard_id) const {
        DCHECK(storage_shard_units_.contains(shard_id));
        return storage_shard_units_.at(shard_id);
    }

    class Sequencer {
    public:
        Sequencer(Sequencer&& other) = default;
        ~Sequencer() {}

        const View* view() const { return view_; }
        uint16_t node_id() const { return node_id_; }

        const View::NodeIdVec& GetStorageShardIds() const {
            return view_->GetStorageShardIds(node_id_);
        }

        const View::NodeIdVec& GetReplicaSequencerNodes() const {
            return replica_sequencer_nodes_;
        }

        bool IsReplicaSequencerNode(uint16_t sequencer_node_id) const {
            return replica_sequencer_node_set_.contains(sequencer_node_id);
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        View::NodeIdVec replica_sequencer_nodes_;
        absl::flat_hash_set<uint16_t> replica_sequencer_node_set_;

        Sequencer(const View* view, uint16_t node_id,
                  const View::NodeIdVec& replica_sequencer_nodes);
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

        // aka my shards
        const View::ShardIdVec& GetStorageShardIds() const {
            return storage_shard_ids_;
        }

        const View::NodeIdVec& GetLocalStorageShardIds(uint16_t sequencer_id) const {
            return local_storage_shard_ids_.at(sequencer_id);
        }

        bool IsStorageShardMember(uint32_t storage_shard_id) const {
            //TODO: improve
            for(uint32_t s : GetStorageShardIds()){
                if(s == storage_shard_id){
                    return true;
                }
            }
            return false;
        }

        const View::NodeIdVec& PickIndexShardNodes() const {
            size_t idx = __atomic_fetch_add(&next_index_shard_, 1, __ATOMIC_RELAXED);
            return index_shard_nodes_.at(idx % view_->num_index_shards());
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        View::ShardIdVec storage_shard_ids_;
        absl::flat_hash_map<uint16_t, View::NodeIdVec> local_storage_shard_ids_;
        std::vector<View::NodeIdVec> index_shard_nodes_;
        mutable size_t next_index_shard_;
        mutable size_t next_index_data_sender;

        Storage(const View* view, uint16_t node_id,
                const View::ShardIdVec storage_shard_ids,
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

        uint16_t PickStorageNode(uint32_t storage_shard_id) const {
            size_t next_storage_node_ = next_shard_storage_node_.at(storage_shard_id);
            size_t idx = __atomic_fetch_add(&next_storage_node_, 1, __ATOMIC_RELAXED);
            size_t storage_node_pos = idx % view_->userlog_replicas_;
            LOG_F(INFO, "Use storage node at position {}", storage_node_pos);
            std::vector<uint16_t> storage_nodes = per_shard_storage_nodes_.at(storage_shard_id);
            DCHECK_EQ(view_->userlog_replicas_, storage_nodes.size());
            return storage_nodes.at(storage_node_pos);
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        //TODO: should be NodeIdVec
        absl::flat_hash_map<uint32_t, std::vector<uint16_t>> per_shard_storage_nodes_;
        absl::flat_hash_map<uint32_t, size_t> next_shard_storage_node_;

        Index(const View* view, uint16_t node_id,
              const absl::flat_hash_map<uint32_t, std::vector<uint16_t>>& storage_shard_nodes);
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
    size_t storage_shards_per_sequencer_;

    NodeIdVec sequencer_node_ids_;
    NodeIdVec storage_node_ids_;
    NodeIdVec index_node_ids_;
    NodeIdVec local_storage_shard_ids_;
    ShardIdVec global_storage_shard_ids_;

    absl::flat_hash_map</*sequencer_id*/uint16_t, NodeIdVec> sequencer_storage_shard_ids_;

    absl::flat_hash_set<uint16_t> active_phylogs_;

    absl::InlinedVector<Sequencer, 16> sequencers_;
    absl::InlinedVector<Storage, 16>   storages_;
    absl::InlinedVector<Index, 16>     indexes_;
    absl::InlinedVector<StorageShard, 16> storage_shards_;

    absl::flat_hash_map<uint32_t, StorageShard*>    storage_shard_units_;

    absl::flat_hash_map<uint16_t, Sequencer*> sequencer_nodes_;
    absl::flat_hash_map<uint16_t, Storage*>   storage_nodes_;
    absl::flat_hash_map<uint16_t, Index*>     index_nodes_;

    uint64_t  log_space_hash_seed_;
    NodeIdVec log_space_hash_tokens_;

    DISALLOW_COPY_AND_ASSIGN(View);
};

class EngineConnection {
public:
    EngineConnection(size_t storage_replication, size_t num_storage_shards) : 
        storage_replication_(storage_replication),
        num_storage_shards_(num_storage_shards),
        local_start_id_(0),
        sequencer_node_(0),
        sequencer_node_set_(false)
        {};
    ~EngineConnection() {}

    void SetSequencerNode(uint16_t sequencer_node, uint32_t local_start_id){
        if (sequencer_node_set_ && sequencer_node_ != sequencer_node){
            LOG_F(ERROR, "Sequencer node gets overwritten from={} to={}", sequencer_node_, sequencer_node); 
        }
        local_start_id_ = local_start_id;
        sequencer_node_ = sequencer_node;
        sequencer_node_set_ = true;
    }

    void AddStorageNode(uint16_t storage_node){
        storage_nodes_.insert(storage_node);
    }

    bool StorageNodesReady(){
        return storage_nodes_.size() == num_storage_shards_ * storage_replication_;
    }

    bool IsReady(){
        return sequencer_node_set_ && StorageNodesReady();
    }

    uint32_t GetLocalStartId(){
        return local_start_id_;
    }

private:
    size_t storage_replication_;
    size_t num_storage_shards_;
    uint32_t local_start_id_;

    absl::flat_hash_set<uint16_t> storage_nodes_;
    uint16_t sequencer_node_;
    bool sequencer_node_set_;
    bool local_start_id_set_;
};

class ViewMutable {
public:
    ViewMutable(){};
    ~ViewMutable(){};

    // for storage and sequencer
    const absl::flat_hash_map<uint32_t, uint16_t> GetStorageShardOccupation() const {
        return current_storage_shard_occupation_;
    }

    void PutStorageShardOccupation(uint32_t storage_shard_id, uint16_t engine_node_id){
        if(current_storage_shard_occupation_.contains(storage_shard_id)){
            LOG_F(INFO, "Storage shard was registered by engine_node={} before", engine_node_id);
            current_storage_shard_occupation_.erase(storage_shard_id);
        }
        current_storage_shard_occupation_.insert({storage_shard_id, engine_node_id});
    }

    bool RemoveStorageShardOccupation(uint32_t storage_shard_id) {
        if(!current_storage_shard_occupation_.contains(storage_shard_id)){
            return false;
        }
        current_storage_shard_occupation_.erase(storage_shard_id);
        return true;
    } 

    // for engine
    const absl::flat_hash_map</*logspace*/uint32_t, const View::StorageShard*> GetMyStorageShards() const {
        return my_storage_shards_;
    }

    const View::StorageShard* GetMyStorageShard(uint32_t logspace_id) const {
        if(my_storage_shards_.contains(logspace_id)){
            return my_storage_shards_.at(logspace_id);
        }
        return nullptr;
    }

    bool UpdateMyStorageShards(uint32_t logspace, const View::StorageShard* storage_shard) {
        if (my_storage_shards_.contains(logspace)){
            return false;
        }
        my_storage_shards_.insert({logspace, storage_shard});
        return true;
    }

    void CreateEngineConnection(uint32_t logspace, size_t storage_replication, size_t num_storage_shards){
        if(engine_connections_.contains(logspace)){
            // todo: is that ok?
            engine_connections_.erase(logspace);
        }
        engine_connections_.insert({
            logspace,
            new EngineConnection(storage_replication, num_storage_shards)
        });
    }

    bool UpdateStorageConnections(uint32_t logspace, uint16_t storage_node_id){
        if (!engine_connections_.contains(logspace)){
            LOG_F(ERROR, "logspace={} is unknown", logspace);
            return false;
        }
        EngineConnection* connection = engine_connections_.at(logspace);
        connection->AddStorageNode(storage_node_id);
        return connection->StorageNodesReady();
    }

    bool UpdateSequencerConnection(uint32_t logspace, uint16_t sequencer_node_id, uint32_t local_start_id){
        if (!engine_connections_.contains(logspace)){
            LOG_F(ERROR, "logspace={} is unknown", logspace);
            return false;
        }
        EngineConnection* connection = engine_connections_.at(logspace);
        connection->SetSequencerNode(sequencer_node_id, local_start_id);
        return true;
    }

    uint32_t GetLocalStartOfConnection(uint32_t logspace){
        if (!engine_connections_.contains(logspace)){
            LOG_F(ERROR, "logspace={} is unknown", logspace);
            return 0;
        }
        return engine_connections_.at(logspace)->GetLocalStartId();
    }

    bool IsEngineActive(uint16_t view_id, std::vector<uint16_t> active_sequencer_nodes) const {
        bool active = false;
        for(uint16_t sequencer_node : active_sequencer_nodes){
            active |= HasEngineLogspace(bits::JoinTwo16(view_id, sequencer_node));
        }
        return active;
    }

    bool HasEngineLogspace(uint32_t logspace_id) const {
        return my_storage_shards_.contains(logspace_id);
    }

    void Reset(){
        NOT_IMPLEMENTED();
        // set everything on empty
    } 

private:
    // used in storages and sequencers
    absl::flat_hash_map<uint32_t, uint16_t> current_storage_shard_occupation_; 
    // used in engines
    absl::flat_hash_map</*logspace_id*/uint32_t, EngineConnection*> engine_connections_;
    absl::flat_hash_map</*logspace_id*/uint32_t, const View::StorageShard*>  my_storage_shards_;

    DISALLOW_COPY_AND_ASSIGN(ViewMutable);
};

}  // namespace log
}  // namespace faas
