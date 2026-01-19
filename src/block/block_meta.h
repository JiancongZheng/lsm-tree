#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

/***
-----------------------------------------------------------------------------
|                               Meta Section                                |
-----------------------------------------------------------------------------
| Entry Numbers(4B) | MetaEntry 1 | MetaEntry 2 | ... | MetaEntry N | Hash |
-----------------------------------------------------------------------------

--------------------------------------------------------------------------------------------
|                                        MetaEntry                                         |
--------------------------------------------------------------------------------------------
| offset(4B) | first key len(2B) | first key(keylen) | last key len(2B) | last key(keylen) |
--------------------------------------------------------------------------------------------
***/

namespace LSMT {
class BlockMeta {
public:
    BlockMeta();

    BlockMeta(size_t offset, const std::string &first_key, const std::string &last_key);

    static void encode_meta(const std::vector<BlockMeta> &meta_entries, std::vector<uint8_t> &meta_data);

    static void decode_meta(const std::vector<uint8_t> &meta_data, std::vector<BlockMeta> &meta_entries);
public:
    size_t offset;
    std::string fkey;
    std::string lkey;
};
} // LOG STRUCTURED MERGE TREE