#include "./cursor.h"
#include "./files.h"

namespace LSMT {
Cursor::Cursor(FileObj* file_obj, size_t offset) : file_obj(file_obj), offset(offset) { }

std::vector<uint8_t> Cursor::read(size_t size) {
    auto buffer = file_obj->read(offset, size);
    offset = offset + size;
    return buffer;
}

uint8_t Cursor::read_uint8() {
    uint8_t value = file_obj->read_uint8(offset);
    offset = offset + sizeof(uint8_t);
    return value;
}

uint16_t Cursor::read_uint16() {
    uint16_t value = file_obj->read_uint16(offset);
    offset = offset + sizeof(uint16_t);
    return value;
}

uint32_t Cursor::read_uint32() {
    uint32_t value = file_obj->read_uint32(offset);
    offset = offset + sizeof(uint32_t);
    return value;
}

uint64_t Cursor::read_uint64() {
    uint64_t value = file_obj->read_uint64(offset);
    offset = offset + sizeof(uint64_t);
    return value;
}

bool Cursor::write(std::vector<uint8_t> &buffer) {
    bool status = file_obj->write(offset, buffer);
    if (status == true) {
        offset = offset + buffer.size();
    }
    return status;
}

bool Cursor::write_uint8(uint8_t value) {
    bool status = file_obj->write_uint8(offset, value);
    if (status == true) {
        offset = offset + sizeof(uint8_t);
    }
    return status;
}

bool Cursor::write_uint16(uint16_t value) {
    bool status = file_obj->write_uint16(offset, value);
    if (status == true) {
        offset = offset + sizeof(uint16_t);
    }
    return status;
}

bool Cursor::write_uint32(uint32_t value) {
    bool status = file_obj->write_uint32(offset, value);
    if (status == true) {
        offset = offset + sizeof(uint32_t);
    }
    return status;
}

bool Cursor::write_uint64(uint64_t value) {
    bool status = file_obj->write_uint64(offset, value);
    if (status == true) {
        offset = offset + sizeof(uint64_t);
    }
    return status;
}

size_t Cursor::get_offset() const {
    return offset;
}

void Cursor::set_offset(size_t offset) {
    offset = offset;
}
} // LOG STRUCTURED MERGE TREE