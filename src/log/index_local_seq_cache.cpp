#include "log/index_local_seq_cache.h"
#include "log/utils.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_cache.h>
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace log {

SeqnumCache::SeqnumCache(int cap_num_rec){
    dbm_.reset(new tkrzw::CacheDBM(int64_t{cap_num_rec}, -1));
    log_header_ = "SeqnumCache: ";
}

SeqnumCache::~SeqnumCache() {}

void SeqnumCache::Put(uint64_t seqnum, uint16_t storage_shard_id){
    std::string key_str = fmt::format("0_{:016x}", seqnum);
    std::string data = fmt::format("{}", storage_shard_id);
    HVLOG_F(1, "Put seqnum={} in cache", bits::HexStr0x(seqnum));
    dbm_->Set(key_str, data, false);
}

bool SeqnumCache::Get(uint64_t seqnum, uint16_t* storage_shard_id){
    std::string key_str = fmt::format("0_{:016x}", seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()){
        int parsed_storage_shard_id;
        if(!absl::SimpleAtoi(data, &parsed_storage_shard_id)){
            LOG(FATAL) << "Cannot parse storage shard id";
        }
        *storage_shard_id = gsl::narrow_cast<uint16_t>(parsed_storage_shard_id);
        return true;
    } else {
        return false;
    }
}

void SeqnumCache::Aggregate(size_t* num_seqnums, size_t* size){
    int64_t num_records = 0;
    dbm_->Count(&num_records);
    *num_seqnums = gsl::narrow_cast<size_t>(num_records);
    *size = gsl::narrow_cast<size_t>(dbm_->GetEffectiveDataSize());
}

IndexQueryResult SeqnumCache::MakeQuery(const IndexQuery& query){
    uint16_t storage_shard_id;
    if(Get(query.query_seqnum, &storage_shard_id)){
        HVLOG_F(1, "Cache hit: Found storage shard id for seqnum={}", bits::HexStr0x(query.query_seqnum));
        return BuildFoundResult(query, query.query_seqnum, storage_shard_id);
    } else {
        HVLOG_F(1, "seqnum={} not in cache", bits::HexStr0x(query.query_seqnum));
        return BuildNotFoundResult(query);
    }
}

IndexQueryResult SeqnumCache::BuildFoundResult(const IndexQuery& query, uint64_t seqnum, uint16_t storage_shard_id){
    uint16_t view_id = log_utils::GetViewId(query.query_seqnum);
    return IndexQueryResult {
        .state = IndexQueryResult::kFound,
        .metalog_progress = 0,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = view_id,
            .storage_shard_id = storage_shard_id,
            .seqnum = seqnum
        }
    };

}
IndexQueryResult SeqnumCache::BuildNotFoundResult(const IndexQuery& query){
   return IndexQueryResult {
        .state = IndexQueryResult::kEmpty,
        .metalog_progress = 0,
        .next_view_id = 0,
        .original_query = query,
        .found_result = IndexFoundResult {
            .view_id = 0,
            .storage_shard_id = 0,
            .seqnum = kInvalidLogSeqNum
        }
    }; 
}

}  // namespace log
}  // namespace faas