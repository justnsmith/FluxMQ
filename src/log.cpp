#include "log.h"

Log::Log() {
    // readFromDisk(dir_path);
}

int Log::append(const std::string &message) {
    std::string length = std::to_string(message.size());
    char deliminator = ',';
    for (int i = 0; i < length.size(); i++) {
        mem[offset + i] = length[i];
    }
    offset += length.size();
    mem[++offset] = deliminator;

    for (int i = 0; i < message.size(); i++) {
        mem[offset + i] = message[i];
    }
    offset += message.size();
    std::ofstream file;
    file.open(dir_path, std::ios::app);
    if (!file) {
        std::cerr << "Can't open file" << std::endl;
        return -1;
    }
    file << length << deliminator << message;
    file.close();
    return offset;
}

std::string read(int givenOffset) {
}
