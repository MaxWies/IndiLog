#include "log/view.h"

namespace faas {
namespace log {

View::View(const ViewProto& view_proto)
    : id_(gsl::narrow_cast<uint16_t>(view_proto.view_id())),
      metalog_replicas_(view_proto.metalog_replicas()),
      userlog_replicas_(view_proto.userlog_replicas()),
      index_replicas_(view_proto.index_replicas()),
      num_index_shards_(view_proto.num_index_shards()),
      num_phylogs_(view_proto.num_phylogs()),
      storage_shards_per_sequencer_(static_cast<size_t>(view_proto.storage_shard_ids_size())),
      sequencer_node_ids_(static_cast<size_t>(view_proto.sequencer_nodes_size())),
      storage_node_ids_(static_cast<size_t>(view_proto.storage_nodes_size())),
      index_node_ids_(static_cast<size_t>(view_proto.index_nodes_size())),
      local_storage_shard_ids_(static_cast<size_t>(view_proto.storage_shard_ids_size())),
      global_storage_shard_ids_(static_cast<size_t>(view_proto.index_nodes_size()) * static_cast<size_t>(view_proto.num_phylogs())),
      log_space_hash_seed_(view_proto.log_space_hash_seed()),
      log_space_hash_tokens_(static_cast<size_t>(view_proto.log_space_hash_tokens_size())) {

    for (size_t i = 0; i < sequencer_node_ids_.size(); i++) {
        sequencer_node_ids_[i] = gsl::narrow_cast<uint16_t>(
            view_proto.sequencer_nodes(static_cast<int>(i)));
        if (i < num_phylogs_) {
            active_phylogs_.insert(sequencer_node_ids_[i]);
        }
    }
    LOG_F(INFO, "View {} has {} physical logs", id_, active_phylogs_.size());
    for (size_t i = 0; i < storage_node_ids_.size(); i++) {
        storage_node_ids_[i] = gsl::narrow_cast<uint16_t>(
            view_proto.storage_nodes(static_cast<int>(i)));
    }
    for (size_t i = 0; i < index_node_ids_.size(); i++) {
        index_node_ids_[i] = gsl::narrow_cast<uint16_t>(
            view_proto.index_nodes(static_cast<int>(i)));
    }
    for (size_t i = 0; i < local_storage_shard_ids_.size(); i++) {
        local_storage_shard_ids_[i] = gsl::narrow_cast<uint16_t>(
            view_proto.storage_shard_ids(static_cast<int>(i)));
    }

    size_t num_local_storage_shards = local_storage_shard_ids_.size();
    size_t num_sequencer_nodes = sequencer_node_ids_.size();
    size_t num_storage_nodes = storage_node_ids_.size();
    size_t num_index_nodes = index_node_ids_.size();

    for (uint16_t sequencer_node : active_phylogs_) {
        sequencer_storage_shard_ids_.insert({
            sequencer_node,
            NodeIdVec(local_storage_shard_ids_.begin(), 
                       local_storage_shard_ids_.end())
        });
    }

    size_t l = 0;
    for (uint16_t sequencer_id : active_phylogs_){
        for (size_t j = 0; j < local_storage_shard_ids_.size(); j++) {
            uint32_t global_storage_shard_id = bits::JoinTwo16(sequencer_id, local_storage_shard_ids_.at(j));
            global_storage_shard_ids_[l+j] = global_storage_shard_id;
        }
        l++;
    }

    absl::flat_hash_set<uint16_t> sequencer_node_id_set(
        sequencer_node_ids_.begin(), sequencer_node_ids_.end());
    DCHECK_EQ(sequencer_node_id_set.size(), num_sequencer_nodes);

    absl::flat_hash_set<uint16_t> storage_node_id_set(
        storage_node_ids_.begin(), storage_node_ids_.end());
    DCHECK_EQ(storage_node_id_set.size(), num_storage_nodes);

    absl::flat_hash_set<uint16_t> index_node_id_set(
        index_node_ids_.begin(), index_node_ids_.end());
    DCHECK_EQ(index_node_id_set.size(), num_index_nodes);

    // TODO: index tier must respect sequence number spaces
    DCHECK_EQ(static_cast<size_t>(view_proto.index_tier_plan_size()),
              num_index_shards_ * index_replicas_);
    absl::flat_hash_map<size_t, std::vector<uint16_t>> index_shard_nodes_tmp;
    for (size_t i = 0; i < num_index_shards_; i++) {
        for (size_t j = 0; j < index_replicas_; j++) {
            uint16_t index_node_id = gsl::narrow_cast<uint16_t>(
                view_proto.index_tier_plan(static_cast<int>(i * index_replicas_ + j)));
            index_shard_nodes_tmp[i].push_back(index_node_id);
        }
    }
    std::vector<NodeIdVec> index_shard_nodes;
    for (size_t i = 0; i < num_index_shards_; i++) {
        index_shard_nodes.push_back(NodeIdVec(index_shard_nodes_tmp[i].begin(), index_shard_nodes_tmp[i].end()));
    }

    DCHECK_EQ(static_cast<size_t>(view_proto.storage_plan_size()), num_local_storage_shards * userlog_replicas_);
    absl::flat_hash_map<uint32_t, std::vector<uint16_t>> storage_nodes;
    absl::flat_hash_map<uint16_t, std::vector<uint32_t>> storage_shard_memberships;
    for (uint16_t sequencer_node : active_phylogs_){
        for (size_t i = 0; i < num_local_storage_shards; i++) {
        uint16_t storage_shard_id = local_storage_shard_ids_[i];
            for (size_t j = 0; j < userlog_replicas_; j++) {
                uint16_t storage_node_id = gsl::narrow_cast<uint16_t>(
                    view_proto.storage_plan(static_cast<int>(i * userlog_replicas_ + j)));
                DCHECK(storage_node_id_set.contains(storage_node_id));
                uint32_t global_storage_shard_id = bits::JoinTwo16(sequencer_node, storage_shard_id);
                storage_nodes[global_storage_shard_id].push_back(storage_node_id);
                storage_shard_memberships[storage_node_id].push_back(global_storage_shard_id);
            }
        }
    }

    // storage shard
    for (size_t i = 0; i < num_local_storage_shards; i++) {
        uint16_t local_storage_shard_id = local_storage_shard_ids_[i];
        for (uint16_t sequencer_node : active_phylogs_){
            uint32_t global_storage_shard_id = bits::JoinTwo16(sequencer_node, local_storage_shard_id);
            storage_shards_.push_back(StorageShard(
                this, global_storage_shard_id,
                NodeIdVec(storage_nodes[global_storage_shard_id].begin(),
                        storage_nodes[global_storage_shard_id].end()),
                sequencer_node,
                index_shard_nodes));
        }
    }

    for (size_t i = 0; i < num_local_storage_shards; i++) {
        for (size_t j = 0; j < active_phylogs_.size(); j++){
            storage_shard_units_[global_storage_shard_ids_[i+j]] = &storage_shards_[i+j];
        }
    }

    // sequencer
    for (size_t i = 0; i < num_sequencer_nodes; i++) {
        std::vector<uint16_t> replica_sequencer_nodes;
        for (size_t j = 1; j < metalog_replicas_; j++) {
            replica_sequencer_nodes.push_back(
                sequencer_node_ids_[(i + j) % num_sequencer_nodes]);
        }
        uint16_t node_id = sequencer_node_ids_[i];
        sequencers_.push_back(Sequencer(
            this, node_id,
            NodeIdVec(replica_sequencer_nodes.begin(),
                      replica_sequencer_nodes.end())));
    }
    for (size_t i = 0; i < num_sequencer_nodes; i++) {
        sequencer_nodes_[sequencer_node_ids_[i]] = &sequencers_[i];
    }

    // storage
    for (size_t i = 0; i < num_storage_nodes; i++) {
        uint16_t node_id = storage_node_ids_[i];
        storages_.push_back(Storage(
            this, node_id,
            ShardIdVec(storage_shard_memberships[node_id].begin(), storage_shard_memberships[node_id].end()),
            index_shard_nodes));
    }
    for (size_t i = 0; i < num_storage_nodes; i++) {
        storage_nodes_[storage_node_ids_[i]] = &storages_[i];
    }

    // index
    for (size_t i = 0; i < num_index_nodes; i++) {
        uint16_t node_id = index_node_ids_[i];
        indexes_.push_back(Index(
            this, node_id,
            storage_nodes));
    }
    for (size_t i = 0; i < num_index_nodes; i++) {
        index_nodes_[index_node_ids_[i]] = &indexes_[i];
    }

    for (size_t i = 0; i < log_space_hash_tokens_.size(); i++) {
        uint16_t node_id = gsl::narrow_cast<uint16_t>(
            view_proto.log_space_hash_tokens(static_cast<int>(i)));
        DCHECK(sequencer_node_id_set.contains(node_id));
        log_space_hash_tokens_[i] = node_id;
    }
}

View::StorageShard::StorageShard(const View* view, uint32_t shard_id,
                     const View::NodeIdVec& storage_nodes,
                     const uint16_t sequencer_node,
                     const std::vector<View::NodeIdVec>& index_shard_nodes)
    : view_(view),
      shard_id_(shard_id),
      storage_nodes_(storage_nodes),
      sequencer_node_(sequencer_node),
      index_shard_nodes_(index_shard_nodes),
      next_storage_node_(0) {
          for(size_t i = 0; i < view->num_index_shards_; i++){
              next_index_replica_node_.push_back(0);
          }
      }

View::Sequencer::Sequencer(const View* view, uint16_t node_id,
                           const View::NodeIdVec& replica_sequencer_nodes)
    : view_(view),
      node_id_(node_id),
      replica_sequencer_nodes_(replica_sequencer_nodes),
      replica_sequencer_node_set_(replica_sequencer_nodes.begin(),
                                  replica_sequencer_nodes.end()) {}

View::Storage::Storage(const View* view, uint16_t node_id, const ShardIdVec storage_shard_ids,
                       const std::vector<View::NodeIdVec>& index_shard_nodes)
    : view_(view),
      node_id_(node_id),
      storage_shard_ids_(storage_shard_ids),
      index_shard_nodes_(index_shard_nodes) {
          //TODO: code improvement
          absl::flat_hash_map<uint16_t, std::vector<uint16_t>> per_sequencer_storage_shards;
          for(uint32_t global_storage_shard_id : storage_shard_ids){
              uint16_t local_storage_shard_id = bits::LowHalf32(global_storage_shard_id);
              uint16_t sequencer_id = bits::HighHalf32(global_storage_shard_id);
              if(per_sequencer_storage_shards.contains(sequencer_id)){
                  per_sequencer_storage_shards.at(sequencer_id).push_back(local_storage_shard_id);
              } else {
                  std::vector<uint16_t> ids {local_storage_shard_id};
                  per_sequencer_storage_shards.insert({sequencer_id, ids});
              }
          }
          for (auto& [k, v] : per_sequencer_storage_shards){
              local_storage_shard_ids_.insert({k, NodeIdVec(v.begin(), v.end())});
          }
      }

View::Index::Index(const View* view, uint16_t node_id,
                   const absl::flat_hash_map<uint32_t, std::vector<uint16_t>>& per_shard_storage_nodes)
    : view_(view),
      node_id_(node_id),
      per_shard_storage_nodes_(per_shard_storage_nodes.begin(),
                            per_shard_storage_nodes.end()){
          for(auto const& p : per_shard_storage_nodes){
              next_shard_storage_node_[p.first] = 0;
          }
      }

}  // namespace log
}  // namespace faas
