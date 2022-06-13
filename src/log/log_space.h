#pragma once

#include "log/log_space_base.h"

namespace faas {
namespace log {

// Used in Sequencer
class MetaLogPrimary final : public LogSpaceBase {
public:
    MetaLogPrimary(const View* view, uint16_t sequencer_id);
    ~MetaLogPrimary();

    uint32_t replicated_metalog_position() const {
        return replicated_metalog_position_;
    }
    bool all_metalog_replicated() const {
        return replicated_metalog_position_ == metalog_position();
    }

    bool BlockShard(uint16_t shard_id, uint32_t* last_cut);
    bool UnblockShard(uint16_t shard_id, uint32_t* last_cut);
    void UpdateStorageProgress(uint16_t storage_id, const std::vector<uint32_t>& progress);
    void UpdateReplicaProgress(uint16_t sequencer_id, uint32_t metalog_position);
    std::optional<MetaLogProto> MarkNextCut();

private:
    absl::flat_hash_set</* shard_id */ uint16_t> dirty_shards_;
    absl::flat_hash_map</* shard_id */ uint16_t, uint32_t> last_cut_;
    absl::flat_hash_map<std::pair</* shard_id */  uint16_t,
                                  /* storage_id */ uint16_t>,
                        uint32_t> shard_progrsses_;

    absl::flat_hash_set</* shard_id */ uint16_t> unblocked_shards_;
    bool blocking_change_;

    absl::flat_hash_map</* sequencer_id */ uint16_t,
                        uint32_t> metalog_progresses_;
    uint32_t replicated_metalog_position_;

    uint32_t GetShardReplicatedPosition(uint16_t storage_shard_id) const;
    void UpdateMetaLogReplicatedPosition();

    DISALLOW_COPY_AND_ASSIGN(MetaLogPrimary);
};

// Used in Sequencer
class MetaLogBackup final : public LogSpaceBase {
public:
    MetaLogBackup(const View* view, uint16_t sequencer_id);
    ~MetaLogBackup();

private:
    DISALLOW_COPY_AND_ASSIGN(MetaLogBackup);
};

// Used in Engine
class LogProducer final : public LogSpaceBase {
public:
    LogProducer(uint16_t engine_id, const View* view, uint16_t sequencer_id, uint32_t metalog_position, uint32_t next_start_id);
    ~LogProducer();

    void LocalAppend(void* caller_data, uint64_t* localid, uint64_t* next_seqnum);

    struct AppendResult {
        uint64_t seqnum;   // seqnum == kInvalidLogSeqNum indicates failure
        uint64_t localid;
        uint64_t metalog_progress;
        void*    caller_data;
    };
    using AppendResultVec = absl::InlinedVector<AppendResult, 4>;
    void PollAppendResults(AppendResultVec* results);

#ifdef __FAAS_OP_TRACING
    const absl::flat_hash_map<uint64_t, void*>& GetPendingAppends(){
        return pending_appends_;
    }
#endif

private:
    uint64_t next_localid_;
    absl::flat_hash_map</* localid */ uint64_t,
                        /* caller_data */ void*> pending_appends_;
    AppendResultVec pending_append_results_;

    void OnNewLogs(uint32_t metalog_seqnum,
                   uint64_t start_seqnum, uint64_t start_localid,
                   uint32_t delta, uint16_t storage_shard_id) override;
    void OnFinalized(uint32_t metalog_position) override;

    DISALLOW_COPY_AND_ASSIGN(LogProducer);
};

// Used in Storage
class LogStorage final : public LogSpaceBase {
public:
    LogStorage(uint16_t storage_id, const View* view, uint16_t sequencer_id);
    ~LogStorage();

    bool Store(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
               std::span<const char> log_data);
    void ReadAt(const protocol::SharedLogMessage& request);

    bool GrabLogEntriesForPersistence(
            std::vector<std::shared_ptr<const LogEntry>>* log_entries,
            uint64_t* new_position) const;
    void LogEntriesPersisted(uint64_t new_position);

    void RemovePendingEntries(uint16_t storage_shard_id);

    struct ReadResult {
        enum Status { kOK, kLookupDB, kFailed };
        Status status;
        std::shared_ptr<const LogEntry> log_entry;
        protocol::SharedLogMessage original_request;
    };
    using ReadResultVec = absl::InlinedVector<ReadResult, 4>;
    void PollReadResults(ReadResultVec* results);

    std::optional<IndexDataPackagesProto> PollIndexData();
    std::optional<std::vector<uint32_t>> GrabShardProgressForSending();

private:
    const View::Storage* storage_node_;

    bool shard_progress_dirty_;
    absl::flat_hash_map</* storage_shard_id */ uint16_t,
                        /* localid          */ uint32_t> shard_progresses_;

    uint64_t persisted_seqnum_position_;
    std::deque<uint64_t> live_seqnums_;
    absl::flat_hash_map</* seqnum */ uint64_t,
                        std::shared_ptr<const LogEntry>>
        live_log_entries_;

    absl::flat_hash_map</* localid */ uint64_t,
                        std::unique_ptr<LogEntry>>
        pending_log_entries_;

    std::multimap</* seqnum */ uint64_t,
                  protocol::SharedLogMessage> pending_read_requests_;
    ReadResultVec pending_read_results_;

    IndexDataProto index_data_;
    IndexDataPackagesProto index_data_packages_;

    void OnNewLogs(uint32_t metalog_seqnum,
                   uint64_t start_seqnum, uint64_t start_localid,
                   uint32_t delta, uint16_t storage_shard_id) override;
    void OnMetaLogApplied(const MetaLogProto& meta_log_proto) override;
    void OnFinalized(uint32_t metalog_position) override;

    void AdvanceShardProgress(uint16_t engine_id);
    void ShrinkLiveEntriesIfNeeded();

    DISALLOW_COPY_AND_ASSIGN(LogStorage);
};

}  // namespace log
}  // namespace faas
