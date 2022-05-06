#pragma once

#include "log/common.h"
#include "log/view.h"
#include "utils/lockable_ptr.h"
#include "utils/object_pool.h"
#include "utils/bits.h"

namespace faas {
namespace log {

class LogSpaceBase {
public:
    virtual ~LogSpaceBase();

    uint16_t view_id() const { return view_->id(); }
    uint16_t sequencer_id() const { return sequencer_node_->node_id(); }
    uint32_t identifier() const { return bits::JoinTwo16(view_id(), sequencer_id()); }

    uint32_t metalog_position() const { return metalog_position_; }
    uint64_t seqnum_position() const {
        return bits::JoinTwo32(identifier(), seqnum_position_);
    }

    std::optional<MetaLogProto> GetMetaLog(uint32_t pos) const;

    // Return true if metalog_position changed
    bool ProvideMetaLog(const MetaLogProto& meta_log_proto);

    bool frozen() const { return state_ == kFrozen; }
    bool finalized() const { return state_ == kFinalized; }

    void Freeze();
    bool Finalize(uint32_t final_metalog_position,
                  const std::vector<MetaLogProto>& tail_metalogs);

    void SerializeToProto(MetaLogsProto* meta_logs_proto);

protected:
    enum Mode { kLiteMode, kFullMode };
    enum State { kCreated, kNormal, kFrozen, kFinalized };

    LogSpaceBase(Mode mode, const View* view, uint16_t sequencer_id);
    void AddInterestedShard(uint16_t shard_id);
    absl::flat_hash_set<size_t> interested_shards_;   

    std::vector<uint16_t> active_storage_shard_ids() { return active_storage_shard_ids_; }

    using OffsetVec = absl::FixedArray<uint32_t>;
    virtual void OnNewLogs(uint32_t metalog_seqnum,
                           uint64_t start_seqnum, uint64_t start_localid,
                           uint32_t delta) {}
    virtual void OnNewLogs(std::vector<std::pair<uint16_t, uint32_t>> productive_cuts) {}
    virtual void OnTrim(uint32_t metalog_seqnum,
                        uint32_t user_logspace, uint64_t user_tag,
                        uint64_t trim_seqnum) {}
    virtual void OnMetaLogApplied(const MetaLogProto& meta_log_proto) {}
    virtual void OnFinalized(uint32_t metalog_position) {}

    Mode mode_;
    State state_;
    const View* view_;
    const View::Sequencer* sequencer_node_;
    uint32_t metalog_position_;
    std::string log_header_;

private:
    std::vector<uint16_t> active_storage_shard_ids_;

    absl::flat_hash_map<uint16_t, uint32_t> shard_progresses_;
    uint32_t seqnum_position_;

    utils::ProtobufMessagePool<MetaLogProto> metalog_pool_;
    std::vector<MetaLogProto*> applied_metalogs_;
    std::map</* metalog_seqnum */ uint32_t, MetaLogProto*> pending_metalogs_;

    bool first_metalog_; //TODO: in fact only for local indexes allowed

    void AdvanceMetaLogProgress();
    bool CanApplyMetaLog(const MetaLogProto& meta_log);
    void ConsiderStorageShardChange(const MetaLogProto& meta_log);
    void ApplyMetaLog(const MetaLogProto& meta_log);

    DISALLOW_COPY_AND_ASSIGN(LogSpaceBase);
};

template<class T>
class PhysicalLogSpaceCollection {
public:
    PhysicalLogSpaceCollection() {}
    ~PhysicalLogSpaceCollection() {}

    LockablePtr<T> GetLogSpaceChecked(uint16_t identifier) const;
    LockablePtr<T> GetLogSpaceChecked(uint32_t identifier) const;

    bool LogSpaceExists(uint16_t identifier) const;
    bool LogSpaceExists(uint32_t identifier) const;

    void InstallLogSpace(std::unique_ptr<T> log_space);

    bool FinalizeLogSpace(uint16_t identifier) const;
    bool FinalizeLogSpace(uint32_t identifier) const;

    using IterCallback = std::function<void(/* identifier */ uint32_t, LockablePtr<T>)>;
    void ForEachLogSpace(IterCallback cb) const;
private:
    //absl::flat_hash_map<uint16_t, std::set<uint16_t>> physical_log_spaces_;
    absl::flat_hash_map</* identifier */ uint32_t, LockablePtr<T>> log_spaces_;
    DISALLOW_COPY_AND_ASSIGN(PhysicalLogSpaceCollection);
};

template<class T>
LockablePtr<T> PhysicalLogSpaceCollection<T>::GetLogSpaceChecked(uint32_t identifier) const {
    if (!log_spaces_.contains(identifier)) {
        LOG_F(FATAL, "Cannot find LogSpace with identifier {}", identifier);
    }
    return log_spaces_.at(identifier);
}

template<class T>
LockablePtr<T> PhysicalLogSpaceCollection<T>::GetLogSpaceChecked(uint16_t identifier) const {
    uint32_t global_identifier = bits::JoinTwo16(0, identifier);
    if (!log_spaces_.contains(global_identifier)) {
        LOG_F(FATAL, "Cannot find LogSpace with identifier {}", global_identifier);
    }
    return log_spaces_.at(global_identifier);
}

template<class T>
bool PhysicalLogSpaceCollection<T>::LogSpaceExists(uint16_t identifier) const {
    uint32_t global_identifier = bits::JoinTwo16(0, identifier);
    return log_spaces_.contains(global_identifier);
}

template<class T>
bool PhysicalLogSpaceCollection<T>::LogSpaceExists(uint32_t identifier) const {
    return log_spaces_.contains(identifier);
}

template<class T>
void PhysicalLogSpaceCollection<T>::InstallLogSpace(std::unique_ptr<T> log_space) {
    uint32_t identifier = log_space->identifier();
    if(log_spaces_.count(identifier) == 0) {
        log_spaces_[identifier] = LockablePtr<T>(std::move(log_space));
    }
}

template<class T>
void PhysicalLogSpaceCollection<T>::ForEachLogSpace(IterCallback cb) const {
    auto iter = log_spaces_.begin();
    while (iter != log_spaces_.end()) {
        DCHECK(log_spaces_.contains(*iter));
        cb(*iter, log_spaces_.at(*iter));
        iter++;
    }
}


template<class T>
class LogSpaceCollection {
public:
    LogSpaceCollection() {}
    ~LogSpaceCollection() {}

    // Return nullptr if not found
    LockablePtr<T> GetLogSpace(uint32_t identifier) const;
    // Panic if not found. It ensures the return is never nullptr
    LockablePtr<T> GetLogSpaceChecked(uint32_t identifier) const;
    LockablePtr<T> GetLogSpaceCheckedShort(uint16_t identifier) const;

    void InstallLogSpace(std::unique_ptr<T> log_space);
    // Only active LogSpace can be finalized
    bool FinalizeLogSpace(uint32_t identifier);
    // Only finalized LogSpace can be removed
    bool RemoveLogSpace(uint32_t identifier);

    using IterCallback = std::function<void(/* identifier */ uint32_t, LockablePtr<T>)>;
    void ForEachActiveLogSpace(const View* view, IterCallback cb) const;
    void ForEachActiveLogSpace(IterCallback cb) const;
    void ForEachFinalizedLogSpace(IterCallback cb) const;

    size_t ComputeSizeLogSpace(uint32_t identifier) const;

private:
    std::set</* identifier */ uint32_t> active_log_spaces_;
    std::set</* identifier */ uint32_t> finalized_log_spaces_;

    absl::flat_hash_map</* identifier */ uint32_t, LockablePtr<T>> log_spaces_;

    DISALLOW_COPY_AND_ASSIGN(LogSpaceCollection);
};

// Start implementation of LogSpaceCollection

template<class T>
LockablePtr<T> LogSpaceCollection<T>::GetLogSpace(uint32_t identifier) const {
    if (log_spaces_.contains(identifier)) {
        return log_spaces_.at(identifier);
    } else {
        return LockablePtr<T>{};
    }
}

template<class T>
LockablePtr<T> LogSpaceCollection<T>::GetLogSpaceChecked(uint32_t identifier) const {
    if (!log_spaces_.contains(identifier)) {
        LOG_F(FATAL, "Cannot find LogSpace with identifier {}", identifier);
    }
    return log_spaces_.at(identifier);
}

template<class T>
void LogSpaceCollection<T>::InstallLogSpace(std::unique_ptr<T> log_space) {
    uint32_t identifier = log_space->identifier();
    DCHECK(active_log_spaces_.count(identifier) == 0);
    active_log_spaces_.insert(identifier);
    log_spaces_[identifier] = LockablePtr<T>(std::move(log_space));
}

template<class T>
bool LogSpaceCollection<T>::FinalizeLogSpace(uint32_t identifier) {
    if (active_log_spaces_.count(identifier) == 0) {
        return false;
    }
    active_log_spaces_.erase(identifier);
    DCHECK(finalized_log_spaces_.count(identifier) == 0);
    finalized_log_spaces_.insert(identifier);
    return true;
}

template<class T>
bool LogSpaceCollection<T>::RemoveLogSpace(uint32_t identifier) {
    if (finalized_log_spaces_.count(identifier) == 0) {
        return false;
    }
    finalized_log_spaces_.erase(identifier);
    DCHECK(log_spaces_.contains(identifier));
    log_spaces_.erase(identifier);
    return true;
}

template<class T>
void LogSpaceCollection<T>::ForEachActiveLogSpace(const View* view, IterCallback cb) const {
    auto iter = active_log_spaces_.lower_bound(bits::JoinTwo16(view->id(), 0));
    while (iter != active_log_spaces_.end()) {
        if (bits::HighHalf32(*iter) > view->id()) {
            break;
        }
        DCHECK(log_spaces_.contains(*iter));
        cb(*iter, log_spaces_.at(*iter));
        iter++;
    }
}

template<class T>
void LogSpaceCollection<T>::ForEachActiveLogSpace(IterCallback cb) const {
    auto iter = active_log_spaces_.begin();
    while (iter != active_log_spaces_.end()) {
        DCHECK(log_spaces_.contains(*iter));
        cb(*iter, log_spaces_.at(*iter));
        iter++;
    }
}

template<class T>
void LogSpaceCollection<T>::ForEachFinalizedLogSpace(IterCallback cb) const {
    auto iter = finalized_log_spaces_.begin();
    while (iter != finalized_log_spaces_.end()) {
        DCHECK(log_spaces_.contains(*iter));
        cb(*iter, log_spaces_.at(*iter));
        iter++;
    }
}

template<class T>
size_t LogSpaceCollection<T>::ComputeSizeLogSpace(uint32_t identifier) const {
    if (log_spaces_.contains(identifier)) {
        auto log_space = log_spaces_.at(identifier);
        return log_space->ComputeSize();
    } else {
        return 0;
    }
}

}  // namespace log
}  // namespace faas
