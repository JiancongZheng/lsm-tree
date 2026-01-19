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

class BlockIterator {
public:
    BlockIterator();

    BlockIterator(std::shared_ptr<Block> block, size_t index, uint64_t trx_id);

    BlockIterator(std::shared_ptr<Block> block, const std::string &key, uint64_t trx_id);

    const std::pair<std::string, std::string>* operator->() const;

    BlockIterator &operator++();

    BlockIterator operator++(int) = delete;

    bool operator==(const BlockIterator &other) const;

    bool operator!=(const BlockIterator &other) const;

    std::pair<std::string, std::string> operator*() const;

    bool is_end();

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