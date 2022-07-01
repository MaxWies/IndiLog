#pragma once

#include "base/common.h"
#include "log/view.h"

namespace faas {
namespace log {

// Stores the process of an engine's registration
class EngineConnection {
public:
    EngineConnection(size_t num_storage_nodes, size_t num_index_nodes, size_t num_aggregator_nodes);
    ~EngineConnection();

    void SetSequencerNode(uint16_t sequencer_node, uint32_t local_start_id);
    void AddStorageNode(uint16_t storage_node);
    void AddIndexNode(uint16_t index_node);
    void AddAggregatorNode(uint16_t aggregator_node);

    bool StorageNodesReady();

    bool IndexNodesReady();
    bool AggregatorNodesReady();

    bool IsReady();

    uint32_t local_start_id() const {
        return local_start_id_;
    }

private:
    size_t num_storage_nodes_;
    size_t num_index_nodes_;
    size_t num_aggregator_nodes_;
    uint32_t local_start_id_;
    absl::flat_hash_set<uint16_t> storage_nodes_;
    absl::flat_hash_set<uint16_t> index_nodes_;
    absl::flat_hash_set<uint16_t> aggregator_nodes_;
    uint16_t sequencer_node_;
    bool sequencer_node_set_;
};

// ViewMutable handles all dynamic changes after a view is installed
class ViewMutable {
public:
    ViewMutable();
    ~ViewMutable();

    /* for storage and sequencer */

    const absl::flat_hash_map<uint32_t, uint16_t> storage_shard_occupation() const {
        return storage_shard_occupation_;
    }

    void PutStorageShardOccupation(uint32_t storage_shard_id, uint16_t engine_node_id);

    bool RemoveStorageShardOccupation(uint32_t storage_shard_id);


    /* for engine */

    const absl::flat_hash_map</*logspace*/uint32_t, const View::StorageShard*> engine_storage_shards() const {
        return engine_storage_shards_;
    }

    const View::StorageShard* GetEngineStorageShard(uint32_t logspace_id) const;
    bool UpdateEngineStorageShards(uint32_t logspace, const View::StorageShard* storage_shard);

    void CreateEngineConnection(uint32_t logspace, size_t num_storage_nodes, size_t num_index_nodes, size_t num_aggregator_nodes);
    bool UpdateStorageConnections(uint32_t logspace, uint16_t storage_node_id);
    bool UpdateIndexConnections(uint32_t logspace, uint16_t index_node_id);
    bool UpdateAggregatorConnections(uint32_t logspace, uint16_t aggregator_node_id);
    bool UpdateSequencerConnection(uint32_t logspace, uint16_t sequencer_node_id, uint32_t local_start_id);
    uint32_t GetLocalStartOfConnection(uint32_t logspace);

    bool IsEngineActive(uint16_t view_id, std::vector<uint16_t> active_sequencer_nodes) const;
    bool HasEngineLogspace(uint32_t logspace_id) const;

    void Reset();

    /* for sequencer */

    void InitializeCurrentEngineNodeIds(uint16_t sequencer_id);
    bool PutCurrentEngineNodeId(uint16_t sequencer_id, uint16_t engine_node_id);
    void RemoveCurrentEngineNodeId(uint16_t engine_node_id);
    absl::flat_hash_set<uint16_t> GetCurrentEngineNodeIds(uint16_t sequencer_id) const;

private:
    // used in storages and sequencers
    absl::flat_hash_map</* storage_shard_id */ uint32_t, /* engine_id */ uint16_t> storage_shard_occupation_; 
    // used in engines
    absl::flat_hash_map</* logspace_id */ uint32_t, EngineConnection*> engine_connections_;
    absl::flat_hash_map</* logspace_id */ uint32_t, const View::StorageShard*>  engine_storage_shards_;
    // used in indexer
    absl::flat_hash_map</* sequencer_id */ uint16_t, /* engine_ids */ absl::flat_hash_set<uint16_t>> per_sequencer_engine_ids_;

    DISALLOW_COPY_AND_ASSIGN(ViewMutable);
};

}  // namespace log
}  // namespace faas
