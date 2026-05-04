#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace LSMT {
class StdFile {
public:
    StdFile() = default;

    ~StdFile() = default;

    bool open(const std::string &filename, bool create);

    bool create(const std::string &filename, std::vector<uint8_t> &buffer);

    void close();

    size_t size();

    bool write(size_t offset, const void *data, size_t size);

    std::vector<uint8_t> read(size_t offset, size_t size);

    bool sync();

    bool remove();

    bool truncate(size_t size);

private:
    std::fstream std_file;
    std::filesystem::path std_filename;
};
} // LOG STRUCTURED MERGE TREE