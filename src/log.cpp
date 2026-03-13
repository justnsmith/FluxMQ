#include "log.h"

Log::Log()
{
    // readFromDisk();
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
    file.write(reinterpret_cast<const char *>(&length), sizeof(uint32_t));
    file.write(message.data(), length);
    file.close();
    int msg_offset = offset;
    offset += sizeof(uint32_t) + length;
    return msg_offset;
}

std::string Log::read(int givenOffset)
{
    uint32_t length;
    std::string message = "";
    std::ifstream file;
    file.open(dir_path, std::ios::binary);
    if (!file) {
        std::cerr << "Can't open file" << std::endl;
        return "";
    }
    file.seekg(givenOffset);
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
    int readOffset = 0;
    std::string currentMessage;
    while (!(currentMessage = read(readOffset)).empty()) {
        uint32_t length = static_cast<uint32_t>(currentMessage.size());
        int needed = static_cast<int>(sizeof(uint32_t)) + static_cast<int>(length);
        if (offset + needed > static_cast<int>(mem.size())) {
            break;
        }
        const char *lenBytes = reinterpret_cast<const char *>(&length);
        for (size_t i = 0; i < sizeof(uint32_t); i++) {
            mem[offset++] = lenBytes[i];
        }
        for (size_t i = 0; i < length; i++) {
            mem[offset++] = currentMessage[i];
        }
        readOffset += needed;
    }
}
