#include "std_file.h"

namespace LSMT {
bool StdFile::open(const std::string &filename, bool create) {
    std_filename = filename;
    if (create == true) {
        std_file.open(std_filename, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
    } else {
        std_file.open(std_filename, std::ios::in | std::ios::out | std::ios::binary);
    }
    return std_file.is_open();
}

bool StdFile::create(const std::string &filename, std::vector<uint8_t> &buffer) {
    if (open(filename, true) == false) {
        throw std::runtime_error("Fail to Open File " + filename + " for Writing");
    }
    write(0, buffer.data(), buffer.size());
    return std_file.is_open();
}

void StdFile::close() {
    if (std_file.is_open()) {
        std_file.flush();
        std_file.close();
    }
}

size_t StdFile::size() {
    return std::filesystem::file_size(std_filename);
}

bool StdFile::write(size_t offset, const void *data, size_t size) {
    std_file.seekp(offset, std::ios::beg);
    std_file.write(static_cast<const char*>(data), size);
    return std_file.good();
}

std::vector<uint8_t> StdFile::read(size_t offset, size_t size) {
    std::vector<uint8_t> buffer(size);
    std_file.seekg(offset, std::ios::beg);
    std_file.read(reinterpret_cast<char*>(buffer.data()), size);
    if (!std_file.good() || !std_file.gcount()) {
        throw std::runtime_error("Fail to Read From STD File");
    }
    return buffer;
}

bool StdFile::sync() {
    if (std_file.is_open() == false) {
        return false;
    }
    std_file.flush();
    return std_file.good();
}

bool StdFile::remove() {
    if (std_file.is_open()) {
        std_file.close();
    }
    return std::filesystem::remove(std_filename);
}

bool StdFile::truncate(size_t size) {
    if (std_file.is_open()) {
        std_file.close();
    }
    std::filesystem::resize_file(std_filename, size);
    std_file.open(std_filename, std::ios::in | std::ios::out | std::ios::binary);
    return std_file.good();
}
} // LOG STRUCTURED MERGE TREE