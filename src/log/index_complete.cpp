#include "log/index_complete.h"

namespace faas {
namespace log {

IndexComplete::IndexComplete(const View* view, uint16_t sequencer_id)
    : Index(view, sequencer_id) {}

IndexComplete::~IndexComplete() {}

void IndexComplete::ProvideIndexData(const IndexDataProto& index_data) {
    int n = index_data.seqnum_halves_size();
    DCHECK_EQ(n, index_data.engine_ids_size());
    DCHECK_EQ(n, index_data.user_logspaces_size());
    DCHECK_EQ(n, index_data.user_tag_sizes_size());
    uint32_t total_tags = absl::c_accumulate(index_data.user_tag_sizes(), 0U);
    DCHECK_EQ(static_cast<int>(total_tags), index_data.user_tags_size());
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
        } else {
#if DCHECK_IS_ON()
            const IndexData& data = received_data_[seqnum];
            DCHECK_EQ(data.engine_id,
                      gsl::narrow_cast<uint16_t>(index_data.engine_ids(i)));
            DCHECK_EQ(data.user_logspace, index_data.user_logspaces(i));
            DCHECK_EQ(data.user_tags.size(),
                      gsl::narrow_cast<size_t>(index_data.user_tag_sizes(i)));
#endif
        }
        tag_iter += num_tags;
    }
    while (received_data_.count(data_received_seqnum_position_) > 0) {
        data_received_seqnum_position_++;
    }
}

void IndexComplete::AdvanceIndexProgress() {
    HVLOG(1) << "Advance index progress";
    while (!cuts_.empty()) {
        uint32_t end_seqnum = cuts_.front().second;
        if (data_received_seqnum_position_ < end_seqnum) {
            HVLOG_F(1, "DataReceivedSeqnumPosition={} is lower then endSeqnum={} of cut.", bits::HexStr0x(data_received_seqnum_position_), bits::HexStr0x(end_seqnum));
            break;
        }
        HVLOG_F(1, "Apply IndexData until seqnum {}", bits::HexStr0x(end_seqnum));
        auto iter = received_data_.begin();
        while (iter != received_data_.end()) {
            uint32_t seqnum = iter->first;
            if (seqnum >= end_seqnum) {
                break;
            }
            const IndexData& index_data = iter->second;
            GetOrCreateIndex(index_data.user_logspace)->Add(
                seqnum, index_data.engine_id, index_data.user_tags);
            iter = received_data_.erase(iter);
        }
        DCHECK_GT(end_seqnum, indexed_seqnum_position_);
        indexed_seqnum_position_ = end_seqnum;
        uint32_t metalog_seqnum = cuts_.front().first;
        indexed_metalog_position_ = metalog_seqnum + 1;
        cuts_.pop_front();
    }
    if (!blocking_reads_.empty()) {
        int64_t current_timestamp = GetMonotonicMicroTimestamp();
        std::vector<std::pair<int64_t, IndexQuery>> unfinished;
        for (const auto& [start_timestamp, query] : blocking_reads_) {
            if (!ProcessBlockingQuery(query)) {
                if (current_timestamp - start_timestamp
                        < absl::ToInt64Microseconds(kBlockingQueryTimeout)) {
                    unfinished.push_back(std::make_pair(start_timestamp, query));
                } else {
                    pending_query_results_.push_back(BuildNotFoundResult(query));
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
}

uint64_t IndexComplete::index_metalog_progress() const {
    return bits::JoinTwo32(identifier(), indexed_metalog_position_);
}

}  // namespace log
}  // namespace faas
