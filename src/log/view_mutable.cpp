#include "log/view_mutable.h"

namespace faas {
namespace log {

EngineConnection::EngineConnection(size_t num_storage_nodes, size_t num_index_nodes, size_t num_aggregator_nodes) : 
        num_storage_nodes_(num_storage_nodes),
        num_index_nodes_(num_index_nodes),
        num_aggregator_nodes_(num_aggregator_nodes),
        local_start_id_(0),
        sequencer_node_(0),
        sequencer_node_set_(false)
        {};

EngineConnection::~EngineConnection() {}

void EngineConnection::SetSequencerNode(uint16_t sequencer_node, uint32_t local_start_id){
    if (sequencer_node_set_ && sequencer_node_ != sequencer_node){
        LOG_F(ERROR, "Sequencer node gets overwritten from={} to={}", sequencer_node_, sequencer_node); 
    }
    local_start_id_ = local_start_id;
    sequencer_node_ = sequencer_node;
    sequencer_node_set_ = true;
}

void EngineConnection::AddStorageNode(uint16_t storage_node){
    storage_nodes_.insert(storage_node);
}

void EngineConnection::AddIndexNode(uint16_t index_node){
    index_nodes_.insert(index_node);
}

void EngineConnection::AddAggregatorNode(uint16_t aggregator_node){
    aggregator_nodes_.insert(aggregator_node);
}

bool EngineConnection::StorageNodesReady(){
    return storage_nodes_.size() == num_storage_nodes_;
}

bool EngineConnection::IndexNodesReady(){
    return index_nodes_.size() == num_index_nodes_;
}

bool EngineConnection::AggregatorNodesReady(){
    return aggregator_nodes_.size() == num_aggregator_nodes_;
}

bool EngineConnection::IsReady(){
    return sequencer_node_set_ && StorageNodesReady() && IndexNodesReady() && AggregatorNodesReady();
}


ViewMutable::ViewMutable() {};

ViewMutable::~ViewMutable() {};

/* for storage and sequencer */

void ViewMutable::PutStorageShardOccupation(uint32_t storage_shard_id, uint16_t engine_node_id){
    if(storage_shard_occupation_.contains(storage_shard_id)){
        LOG_F(INFO, "Storage shard was registered by engine_node={} before", engine_node_id);
        storage_shard_occupation_.erase(storage_shard_id);
    }
    storage_shard_occupation_.insert({storage_shard_id, engine_node_id});
}

bool ViewMutable::RemoveStorageShardOccupation(uint32_t storage_shard_id) {
    if(!storage_shard_occupation_.contains(storage_shard_id)){
        return false;
    }
    storage_shard_occupation_.erase(storage_shard_id);
    return true;
}

/* for engine */

const View::StorageShard* ViewMutable::GetEngineStorageShard(uint32_t logspace_id) const {
    if(engine_storage_shards_.contains(logspace_id)){
        return engine_storage_shards_.at(logspace_id);
    }
    return nullptr;
}

bool ViewMutable::UpdateEngineStorageShards(uint32_t logspace, const View::StorageShard* storage_shard) {
    if (engine_storage_shards_.contains(logspace)){
        return false;
    }
    engine_storage_shards_.insert({logspace, storage_shard});
    return true;
}

void ViewMutable::CreateEngineConnection(uint32_t logspace, size_t num_storage_nodes, size_t num_index_nodes, size_t num_aggregator_nodes){
    if(engine_connections_.contains(logspace)){
        delete engine_connections_.at(logspace);
        engine_connections_.erase(logspace);
    }
    engine_connections_.insert({
        logspace,
        new EngineConnection(num_storage_nodes, num_index_nodes, num_aggregator_nodes)
    });
}

bool ViewMutable::UpdateStorageConnections(uint32_t logspace, uint16_t storage_node_id){
    if (!engine_connections_.contains(logspace)){
        LOG_F(ERROR, "logspace={} is unknown", logspace);
        return false;
    }
    EngineConnection* connection = engine_connections_.at(logspace);
    connection->AddStorageNode(storage_node_id);
    return connection->IsReady();
}

bool ViewMutable::UpdateIndexConnections(uint32_t logspace, uint16_t index_node_id){
    if (!engine_connections_.contains(logspace)){
        LOG_F(ERROR, "logspace={} is unknown", logspace);
        return false;
    }
    EngineConnection* connection = engine_connections_.at(logspace);
    connection->AddIndexNode(index_node_id);
    return connection->IsReady();
}

bool ViewMutable::UpdateAggregatorConnections(uint32_t logspace, uint16_t aggregator_node_id){
    if (!engine_connections_.contains(logspace)){
        LOG_F(ERROR, "logspace={} is unknown", logspace);
        return false;
    }
    EngineConnection* connection = engine_connections_.at(logspace);
    connection->AddAggregatorNode(aggregator_node_id);
    return connection->IsReady();
}

bool ViewMutable::UpdateSequencerConnection(uint32_t logspace, uint16_t sequencer_node_id, uint32_t local_start_id){
    if (!engine_connections_.contains(logspace)){
        LOG_F(ERROR, "logspace={} is unknown", logspace);
        return false;
    }
    EngineConnection* connection = engine_connections_.at(logspace);
    connection->SetSequencerNode(sequencer_node_id, local_start_id);
    return true;
}

uint32_t ViewMutable::GetLocalStartOfConnection(uint32_t logspace){
    if (!engine_connections_.contains(logspace)){
        LOG_F(ERROR, "logspace={} is unknown", logspace);
        return 0;
    }
    return engine_connections_.at(logspace)->local_start_id();
}

bool ViewMutable::IsEngineActive(uint16_t view_id, std::vector<uint16_t> active_sequencer_nodes) const {
    bool active = false;
    for(uint16_t sequencer_node : active_sequencer_nodes){
        active |= HasEngineLogspace(bits::JoinTwo16(view_id, sequencer_node));
    }
    return active;
}

bool ViewMutable::HasEngineLogspace(uint32_t logspace_id) const {
    return engine_storage_shards_.contains(logspace_id);
}

void ViewMutable::Reset(){
    NOT_IMPLEMENTED();
}

/* for sequencer */

void ViewMutable::InitializeCurrentEngineNodeIds(uint16_t sequencer_id) {
    if (per_sequencer_engine_ids_.contains(sequencer_id)){
        per_sequencer_engine_ids_.at(sequencer_id).clear();
    } 
    per_sequencer_engine_ids_.insert({sequencer_id, {}});
}

bool ViewMutable::PutCurrentEngineNodeId(uint16_t sequencer_id, uint16_t engine_node_id) {
    if (!per_sequencer_engine_ids_.contains(sequencer_id)){
        LOG_F(FATAL, "Sequencer id {} is not known", sequencer_id);
    }
    if (per_sequencer_engine_ids_.at(sequencer_id).contains(engine_node_id)){
        return false;
    }
    per_sequencer_engine_ids_.at(sequencer_id).insert(engine_node_id);
    return true;
} 

void ViewMutable::RemoveCurrentEngineNodeId(uint16_t engine_node_id) {
    for (auto& [sequencer_id, engine_ids] : per_sequencer_engine_ids_){
        if (engine_ids.contains(engine_node_id)){
            engine_ids.erase(engine_node_id);
        }
    }
} 

absl::flat_hash_set<uint16_t> ViewMutable::GetCurrentEngineNodeIds(uint16_t sequencer_id) const {
    if (!per_sequencer_engine_ids_.contains(sequencer_id)){
        LOG_F(FATAL, "Sequencer id {} is not known", sequencer_id);
    }
    return per_sequencer_engine_ids_.at(sequencer_id);
}

}  // namespace log
}  // namespace faas
