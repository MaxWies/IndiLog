#pragma once

#include "log/index_dto.h"

// Forward declarations
namespace tkrzw { class CacheDBM; }

namespace faas {
namespace log {

class SeqnumCache {
public:
    explicit SeqnumCache(int cap_num_rec);
    ~SeqnumCache();

    void Put(uint64_t seqnum, uint16_t storage_shard_id);
    bool Get(uint64_t seqnum, uint16_t* storage_shard_id);
    void Clear();
    void Aggregate(size_t* num_seqnums, size_t* size);
    IndexQueryResult MakeQuery(const IndexQuery& query);

private:
    std::unique_ptr<tkrzw::CacheDBM> dbm_;
    std::string log_header_;

    IndexQueryResult BuildFoundResult(const IndexQuery& query, uint64_t seqnum, uint16_t storage_shard_id);
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query);

    DISALLOW_COPY_AND_ASSIGN(SeqnumCache);
};

}  // namespace log
}  // namespace faas