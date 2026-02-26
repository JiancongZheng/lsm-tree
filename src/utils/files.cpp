// #include "./cursor.h"
#include "./files.h"

namespace LSMT {
FileObj::FileObj() : file(std::make_unique<StdFile>()) { }

FileObj::~FileObj() = default;

FileObj::FileObj(FileObj &&other) noexcept {
    if (this != &other) {
        file = std::move(other.file);
    }
}

FileObj &FileObj::operator=(FileObj &&other) noexcept {
    if (this != &other) {
        file = std::move(other.file);
    }
    return *this;
}

size_t FileObj::size() const {
    return file->size();
}

void FileObj::remove() {
    file->remove();
}

bool FileObj::truncate(size_t size) {
    if (size > file->size()) {
        throw std::out_of_range("Truncate Offset Out Of File Size");
    }
    return file->truncate(size);
}

FileObj FileObj::create_and_write(const std::string &path, std::vector<uint8_t> buffer) {
    FileObj file_obj;

    if (file_obj.file->create(path, buffer) == false) {
        throw std::runtime_error("Failed To Create or Write File " + path);
    }
    file_obj.file->sync();

    return file_obj;  // 优先使用NRVO 如果未启用则尝试移动构造return std::move(file_obj)
}

FileObj FileObj::open(const std::string &path, bool create) {
    FileObj file_obj;

    if (file_obj.file->open(path, create) == false) {
        throw std::runtime_error("Failed To Open File " + path);
    }

    return file_obj;  // 优先使用NRVO 如果未启用则尝试移动构造return std::move(file_obj)
}

std::vector<uint8_t> FileObj::read(size_t offset, size_t size) {
    if (offset + size > file->size()) {
        throw std::out_of_range("Read Beyond File Size");
    }
    return file->read(offset, size);
}

uint8_t FileObj::read_uint8(size_t offset) {
    if (offset + sizeof(uint8_t) > file->size()) {
        throw std::out_of_range("Read Beyond File Size");
    }
    auto result = file->read(offset, sizeof(uint8_t));
    return static_cast<uint8_t>(result[0]);
}

uint16_t FileObj::read_uint16(size_t offset) {
    if (offset + sizeof(uint16_t) > file->size()) {
        throw std::out_of_range("Read Beyond File Size");
    }
    auto buffer = file->read(offset, sizeof(uint16_t));
    return std::accumulate(buffer.begin(), buffer.end(), uint16_t{0},
        [](uint16_t res, uint8_t val) { return (res << 8) | static_cast<uint16_t>(val); });
    
}

uint32_t FileObj::read_uint32(size_t offset) {
    if (offset + sizeof(uint32_t) > file->size()) {
        throw std::out_of_range("Read Beyond File Size");
    }
    auto buffer = file->read(offset, sizeof(uint32_t));
    return std::accumulate(buffer.begin(), buffer.end(), uint32_t{0},
        [](uint32_t res, uint8_t val) { return (res << 8) | static_cast<uint32_t>(val); });
}

uint64_t FileObj::read_uint64(size_t offset) {
    if (offset + sizeof(uint64_t) > file->size()) {
        throw std::out_of_range("Read Beyond File Size");
    }
    auto buffer = file->read(offset, sizeof(uint64_t));
    return std::accumulate(buffer.begin(), buffer.end(), uint64_t{0},
        [](uint32_t res, uint8_t val) { return (res << 8) | static_cast<uint64_t>(val); });
}

bool FileObj::write(size_t offset, std::vector<uint8_t> &buffer) {
    return file->write(offset, buffer.data(), buffer.size());
}

bool FileObj::write_uint8(size_t offset, uint8_t value) {
    uint8_t content[sizeof(uint8_t)];
    for (size_t i = sizeof(uint8_t); i-- > 0;) {
        content[i] = static_cast<uint8_t>(value);
        value = value >> 8;
    }
    return file->write(offset, content, sizeof(uint8_t));
}

bool FileObj::write_uint16(size_t offset, uint16_t value) {
    uint8_t content[sizeof(uint16_t)];
    for (size_t i = sizeof(uint16_t); i-- > 0;) {
        content[i] = static_cast<uint8_t>(value);
        value = value >> 8;
    }
    return file->write(offset, content, sizeof(uint16_t));
}

bool FileObj::write_uint32(size_t offset, uint32_t value) {
    uint8_t content[sizeof(uint32_t)];
    for (size_t i = sizeof(uint32_t); i-- > 0;) {
        content[i] = static_cast<uint8_t>(value);
        value = value >> 8;
    }
    return file->write(offset, content, sizeof(uint32_t));
}

bool FileObj::write_uint64(size_t offset, uint64_t value) {
    uint8_t content[sizeof(uint64_t)];
    for (size_t i = sizeof(uint64_t); i-- > 0;) {
        content[i] = static_cast<uint8_t>(value);
        value = value >> 8;
    }
    return file->write(offset, content, sizeof(uint64_t));
}

bool FileObj::append(std::vector<uint8_t> &buffer) {
    return file->write(file->size(), buffer.data(), buffer.size());
}

bool FileObj::append_uint8(uint8_t value) {
    uint8_t content[sizeof(uint8_t)];
    for (size_t i = sizeof(uint8_t); i-- > 0;) {
        content[i] = static_cast<uint8_t>(value);
        value = value >> 8;
    } 
    return file->write(file->size(), content, sizeof(uint8_t));
}

bool FileObj::append_uint16(uint16_t value) {
    uint8_t content[sizeof(uint8_t)];
    for (size_t i = sizeof(uint16_t); i-- > 0;) {
        content[i] = static_cast<uint8_t>(value);
        value = value >> 8;
    }
    return file->write(file->size(), content, sizeof(uint16_t));
}

bool FileObj::append_uint32(uint32_t value) {
    uint8_t content[sizeof(uint8_t)];
    for (size_t i = sizeof(uint32_t); i-- > 0;) {
        content[i] = static_cast<uint8_t>(value);
        value = value >> 8;
    }
    return file->write(file->size(), content, sizeof(uint32_t));
}

bool FileObj::append_uint64(uint64_t value) {
    uint8_t content[sizeof(uint64_t)];
    for (size_t i = sizeof(uint64_t); i-- > 0;) {
        content[i] = static_cast<uint8_t>(value);
        value = value >> 8;
    }
    return file->write(file->size(), content, sizeof(uint64_t));
}

bool FileObj::sync() {
    return file->sync();
}

Cursor FileObj::get_cursor(FileObj &file_obj) {
    return Cursor(&file_obj, 0);
}
} // LOG STRUCTURED MERGE TREE