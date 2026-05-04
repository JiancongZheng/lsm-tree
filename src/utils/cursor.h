#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <vector>

namespace LSMT {
class FileObj;

class Cursor {
public:
    explicit Cursor(FileObj* file_obj, size_t offset = 0);

    std::vector<uint8_t> read(size_t size);

    uint8_t read_uint8();

    uint16_t read_uint16();

    uint32_t read_uint32();

    uint64_t read_uint64();

    bool write(std::vector<uint8_t> &buffer);

    bool write_uint8(uint8_t value);

    bool write_uint16(uint16_t value);

    bool write_uint32(uint32_t value);

    bool write_uint64(uint64_t value);

    size_t get_offset() const;

    void set_offset(size_t offset);

private:
    std::shared_ptr<FileObj> file_obj;
    size_t offset;
};
} // LOG STRUCTURED MERGE TREE