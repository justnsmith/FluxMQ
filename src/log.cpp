#include "log.h"

Log::Log()
{
    readFromDisk();
}

int Log::append(const std::string &message)
{
    // Read the message size into 4 byte length
    uint32_t length = message.size();
    std::ofstream file;
    file.open(dir_path, std::ios::binary | std::ios::app);
    if (!file) {
        std::cerr << "Can't open file" << std::endl;
        return -1;
    }

    // First 4 bytes written are the N bytes of the message size
    index_[next_offset_] = curr_byte_pos;
    file.write(reinterpret_cast<const char *>(&length), sizeof(uint32_t));
    file.write(message.data(), length);
    file.close();
    curr_byte_pos += sizeof(uint32_t) + length;
    return next_offset_++;
}

std::string Log::read(uint64_t offset)
{
    uint32_t length;
    std::string message = "";
    std::ifstream file;
    file.open(dir_path, std::ios::binary);
    if (!file) {
        std::cerr << "Can't open file" << std::endl;
        return "";
    }
    auto it = index_.find(offset);
    if (it == index_.end())
        return "";
    file.seekg(it->second);
    file.read(reinterpret_cast<char *>(&length), sizeof(length));
    if (file.fail()) {
        return "";
    }
    message.resize(length);
    file.read(message.data(), length);
    return message;
}

void Log::readFromDisk()
{
    std::ifstream file;
    file.open(dir_path, std::ios::binary);
    if (!file) {
        return;
    }

    uint32_t length;
    while (file.read(reinterpret_cast<char *>(&length), sizeof(length))) {
        index_[next_offset_] = curr_byte_pos;
        curr_byte_pos += sizeof(uint32_t) + length;
        next_offset_++;
        file.seekg(length, std::ios::cur);
        if (file.fail()) {
            break;
        }
    }
}
