#pragma once

#include "log/index.h"

namespace faas {
namespace log {

class IndexComplete final : public Index {
    
public:
    IndexComplete(const View* view, uint16_t sequencer_id);
    ~IndexComplete();
    void ProvideIndexData(const IndexDataProto& index_data);
    void AdvanceIndexProgress();
private:
    uint64_t index_metalog_progress() const override;
    DISALLOW_COPY_AND_ASSIGN(IndexComplete);
};

}  // namespace log
}  // namespace faas
