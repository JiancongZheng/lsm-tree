#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include <utility>

#include "../iterator/iterator.h"
#include "../block/block_iterator.h"

namespace LSMT {
class SST;

class SSTIterator : public BaseIterator {
    friend class SST;

public:
    SSTIterator(std::shared_ptr<SST> sst, uint64_t trx_id);

    SSTIterator(std::shared_ptr<SST> sst, const std::string &key, uint64_t trx_id);

    IteratorItem* operator->() const;
    
    virtual IteratorItem operator*() const override;

    virtual BaseIterator &operator++() override;

    virtual bool operator==(const BaseIterator &other) const override;

    virtual bool operator!=(const BaseIterator &other) const override;

    virtual IteratorType get_iterator_type() const override;

    virtual uint64_t get_trx_id() const;

    virtual bool is_end() const override;

    virtual bool is_vld() const override;

    std::string get_key();

    std::string get_val();

    static std::pair<HeapIterator, HeapIterator>
    merge_sst_iterator(std::vector<SSTIterator> iters, uint64_t trx_id);

private:
    void update_current() const;

private:
    std::shared_ptr<SST> sst;
    size_t block_id;
    uint64_t max_trx_id;
    std::shared_ptr<BlockIterator> block_it;
    mutable std::optional<std::pair<std::string, std::string>> cached_value;
};
} // LOG STRUCTURED MERGE TREE