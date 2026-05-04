#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "cursor.h"
#include "std_file.h"

namespace LSMT {
class Cursor;

class FileObj {
public:
    FileObj();

    ~FileObj();

    FileObj(const FileObj &other) = delete;

    FileObj &operator=(const FileObj &other) = delete;

    FileObj(FileObj &&other) noexcept;

    FileObj &operator=(FileObj &&other) noexcept;

    size_t size() const;

    void remove();

    bool truncate(size_t size);

    static FileObj create_and_write(const std::string &path, std::vector<uint8_t> buffer);

    static FileObj open(const std::string &path, bool create);

    std::vector<uint8_t> read(size_t offset, size_t size);

    uint8_t read_uint8(size_t offset);

    uint16_t read_uint16(size_t offset);

    uint32_t read_uint32(size_t offset);

    uint64_t read_uint64(size_t offset);

    bool write(size_t offset, std::vector<uint8_t> &buffer);

    bool write_uint8(size_t offset, uint8_t value);

    bool write_uint16(size_t offset, uint16_t value);

    bool write_uint32(size_t offset, uint32_t value);

    bool write_uint64(size_t offset, uint64_t value);

    bool append(std::vector<uint8_t> &buffer);

    bool append_uint8(uint8_t value);
  
    bool append_uint16(uint16_t value);
    
    bool append_uint32(uint32_t value);
    
    bool append_uint64(uint64_t value);

    bool sync();

    Cursor get_cursor(FileObj &file_obj);

private:
    std::unique_ptr<StdFile> file;
};

} // LOG STRUCTURED MERGE TREE