#include "log/log_space_base.h"

#include "utils/bits.h"

namespace faas {
namespace log {

LogSpaceBase::LogSpaceBase(Mode mode, const View* view, uint16_t sequencer_id)
    : mode_(mode),
      state_(kCreated),
      view_(view),
      sequencer_node_(view->GetSequencerNode(sequencer_id)),
      metalog_position_(0),
      log_header_(fmt::format("LogSpace[{}-{}]: ", view->id(), sequencer_id)),
      seqnum_position_(0) {
        for(uint16_t local_storage_shard_id : view->GetLocalStorageShardIds()){
            shard_progresses_.insert({local_storage_shard_id, 0});
        }
      }

LogSpaceBase::~LogSpaceBase() {}

void LogSpaceBase::AddInterestedShard(uint16_t shard_id) {
    DCHECK(state_ == kCreated);
    if(interested_shards_.contains(shard_id)){
        HLOG_F(WARNING, "Shard {} is already added to the interested shards", shard_id);
        return;
    }
    interested_shards_.insert(shard_id);
}

std::optional<MetaLogProto> LogSpaceBase::GetMetaLog(uint32_t pos) const {
    DCHECK(mode_ == kFullMode);
    if (pos >= metalog_position_) {
        return std::nullopt;
    }
    return *applied_metalogs_.at(pos);
}

bool LogSpaceBase::ProvideMetaLog(const MetaLogProto& meta_log) {
    DCHECK(state_ == kNormal || state_ == kFrozen);
    if (meta_log.type() == MetaLogProto::TRIM && (mode_ == kLogStorage || mode_ == kLogProducer || mode_ == kLogSuffix)) {
        HLOG_F(WARNING, "Trim log (seqnum={}) is simply ignore in lite mode",
               meta_log.metalog_seqnum());
        return false;
    }
    uint32_t seqnum = meta_log.metalog_seqnum();
    if (seqnum < metalog_position_) {
        HVLOG_F(1, "MetalogUpdate: metalog_seqnum={} lower than my position={}", seqnum, metalog_position_);
        return false;
    }
    HVLOG_F(1, "MetalogUpdate: Apply metalog_seqnum={}", seqnum);
    MetaLogProto* meta_log_copy = metalog_pool_.Get();
    meta_log_copy->CopyFrom(meta_log);
    pending_metalogs_[seqnum] = meta_log_copy;
    uint32_t prev_metalog_position = metalog_position_;
    AdvanceMetaLogProgress();
    return metalog_position_ > prev_metalog_position;
}

void LogSpaceBase::Freeze() {
    DCHECK(state_ == kNormal);
    state_ = kFrozen;
}

bool LogSpaceBase::Finalize(uint32_t final_metalog_position,
                            const std::vector<MetaLogProto>& tail_metalogs) {
    DCHECK(state_ == kNormal || state_ == kFrozen);
    HLOG_F(INFO, "Finalize log space, final_position={}, provided_tails={}",
           final_metalog_position, tail_metalogs.size());
    if (metalog_position_ == final_metalog_position) {
        OnFinalized(metalog_position_);
        return true;
    }
    if (metalog_position_ > final_metalog_position) {
        if (metalog_position_ > final_metalog_position + 1) {
            HLOG_F(FATAL, "See the future: current_position={}, expected_position={}",
                   metalog_position_, final_metalog_position);
        }
        // TODO: try fix this
        HLOG(WARNING) << "Fine, the problem with primary sequencer";
        OnFinalized(metalog_position_);
        return true;
    }
    for (const MetaLogProto& meta_log : tail_metalogs) {
        ProvideMetaLog(meta_log);
    }
    if (metalog_position_ < final_metalog_position) {
        HLOG_F(ERROR, "Metalog entries not sufficient: current_position={}, expected_position={}",
               metalog_position_, final_metalog_position);
        return false;
    } else if (metalog_position_ > final_metalog_position) {
        HLOG_F(FATAL, "It's uncommon, current_position={}, expected_position={}",
               metalog_position_, final_metalog_position);
    }
    OnFinalized(metalog_position_);
    return true;
}

void LogSpaceBase::SerializeToProto(MetaLogsProto* meta_logs_proto) {
    DCHECK(state_ == kFinalized && mode_ == kFullMode);
    meta_logs_proto->Clear();
    meta_logs_proto->set_logspace_id(identifier());
    for (const MetaLogProto* metalog : applied_metalogs_) {
        meta_logs_proto->add_metalogs()->CopyFrom(*metalog);
    }
}

void LogSpaceBase::AdvanceMetaLogProgress() {
    //pending metalogs is always sorted
    auto iter = pending_metalogs_.begin();
    while (iter != pending_metalogs_.end()) {
        if (iter->first < metalog_position_) {
            iter = pending_metalogs_.erase(iter);
            continue;
        }
        MetaLogProto* meta_log = iter->second;
        if (meta_log->metalog_seqnum() != metalog_position_){
            break;
        }
        ApplyMetaLog(*meta_log);
        switch (mode_) {
        case kLogProducer:
        case kLogStorage:
        case kLogSuffix:
            metalog_pool_.Return(meta_log);
            break;
        case kFullMode:
            DCHECK_EQ(size_t{metalog_position_}, applied_metalogs_.size());
            applied_metalogs_.push_back(meta_log);
            break;
        default:
            UNREACHABLE();
        }
        metalog_position_ = meta_log->metalog_seqnum() + 1;
        OnMetaLogApplied(*meta_log);
        iter = pending_metalogs_.erase(iter);
    }
}

bool LogSpaceBase::CanApplyMetaLog(const MetaLogProto& meta_log) {
    switch (mode_) {
    case kLogProducer:
        switch (meta_log.type()) {
        case MetaLogProto::NEW_LOGS:
            for (int i = 0; i < meta_log.productive_storage_shard_ids_size(); i++) {
                uint32_t storage_shard = meta_log.productive_storage_shard_ids().at(i);
                uint16_t local_storage_shard = gsl::narrow_cast<uint16_t>(storage_shard);
                if (interested_shards_.contains(local_storage_shard)){
                    uint32_t shard_start = meta_log.new_logs_proto().shard_starts(
                        static_cast<int>(i));
                    DCHECK_GE(shard_start, shard_progresses_[local_storage_shard]);
                    if (shard_start > shard_progresses_[local_storage_shard]) {
                        HVLOG_F(1, "Cannot apply metalog. shard_start={} higher than shard_progress={}", shard_start, shard_progresses_[local_storage_shard]);
                        return false;
                    }
                }
            }
            return true;
        default:
            break;
        }
        break;
    case kLogStorage:
    case kFullMode:
    case kLogSuffix:
        return meta_log.metalog_seqnum() == metalog_position_;
    default:
        break;
    }
    UNREACHABLE();
}

void LogSpaceBase::ApplyMetaLog(const MetaLogProto& meta_log) {
    switch (meta_log.type()) {
    case MetaLogProto::NEW_LOGS:
        {
            switch (mode_) {
            case kLogSuffix:
                {
                    const auto& new_logs = meta_log.new_logs_proto();
                    uint32_t start_seqnum = new_logs.start_seqnum();
                    HVLOG_F(1, "MetalogUpdate: Apply NEW_LOGS meta log: metalog_seqnum={}, start_seqnum={}",
                            meta_log.metalog_seqnum(), start_seqnum);
                    std::vector<std::pair<uint16_t, uint32_t>> productive_cuts_;
                    for (int i = 0; i < meta_log.productive_storage_shard_ids_size(); i++) {
                        uint16_t storage_shard_id = gsl::narrow_cast<uint16_t>(meta_log.productive_storage_shard_ids(i));
                        uint32_t shard_start = new_logs.shard_starts(i);
                        uint32_t delta = new_logs.shard_deltas(i);
                        shard_progresses_[storage_shard_id] = shard_start + delta;
                        start_seqnum += delta;
                        productive_cuts_.push_back(std::make_pair(storage_shard_id, start_seqnum - 1));
                    }
                    DCHECK_GT(start_seqnum, seqnum_position_);
                    seqnum_position_ = start_seqnum;
                    OnNewLogs(productive_cuts_);
                    break;
                }
            default:
                {
                    const auto& new_logs = meta_log.new_logs_proto();
                    uint32_t start_seqnum = new_logs.start_seqnum();
                    HVLOG_F(1, "MetalogUpdate: Apply NEW_LOGS meta log: metalog_seqnum={}, start_seqnum={}",
                            meta_log.metalog_seqnum(), start_seqnum);
                    for (int i = 0; i < meta_log.productive_storage_shard_ids_size(); i++) {
                        uint16_t storage_shard_id = gsl::narrow_cast<uint16_t>(meta_log.productive_storage_shard_ids(i));
                        uint32_t shard_start = new_logs.shard_starts(i);
                        uint32_t delta = new_logs.shard_deltas(i);
                        uint64_t start_localid = bits::JoinTwo32(storage_shard_id, shard_start);
                        if (mode_ == kFullMode || interested_shards_.contains(storage_shard_id)) { // interested shards important for logproducer and logstorage
                            OnNewLogs(meta_log.metalog_seqnum(), 
                                bits::JoinTwo32(identifier(), start_seqnum),
                                start_localid, delta, storage_shard_id);
                        }
                        shard_progresses_[storage_shard_id] = shard_start + delta;
                        start_seqnum += delta;
                    }
                    DCHECK_GT(start_seqnum, seqnum_position_);
                    seqnum_position_ = start_seqnum;
                    break;
                }
            }
        }
        break;
    case MetaLogProto::TRIM:
        DCHECK(mode_ == kFullMode);
        {
            const auto& trim = meta_log.trim_proto();
            OnTrim(meta_log.metalog_seqnum(),
                   trim.user_logspace(), trim.user_tag(),
                   trim.trim_seqnum());
        }
        break;
    default:
        UNREACHABLE();
    }
}

}  // namespace log
}  // namespace faas
