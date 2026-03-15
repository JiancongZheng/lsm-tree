#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <stdexcept>

#include "../iterator/iterator.h"

namespace LSMT {
class Block;

class BlockIterator : public BaseIterator {
public:
    BlockIterator();

    BlockIterator(std::shared_ptr<Block> block, size_t index, uint64_t trx_id);

    BlockIterator(std::shared_ptr<Block> block, const std::string &key, uint64_t trx_id);

    const IteratorItem* operator->() const;
    
    std::pair<std::string, std::string> operator*() const override;

    BlockIterator &operator++() override;

    BlockIterator operator++(int) = delete;

    bool operator==(const BaseIterator &other) const override;

    bool operator!=(const BaseIterator &other) const override;

    IteratorType get_iterator_type() const override;

    uint64_t get_trx_id() const override;

    bool is_end() const override;

    bool is_vld() const override;

private:
    void update_current() const;

    void skip_by_trx_id();

private:
    std::shared_ptr<Block> block;
    size_t curr_index;
    uint64_t trx_id;
    mutable std::optional<std::pair<std::string, std::string>> cached_kvpair;
};
} // LOG STRUCTURED MERGE TREE