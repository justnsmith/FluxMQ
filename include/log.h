#include <cstdint>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

class Log
{
  private:
    std::unordered_map<uint64_t, uint64_t> index_;
    std::string dir_path = "data/mem.bin";
    uint64_t next_offset_{};
    uint64_t curr_byte_pos{};

    void readFromDisk();

  public:
    Log();
    int append(const std::string &message);
    std::string read(uint64_t offset);
};
