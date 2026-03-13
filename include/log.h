#include <array>
#include <fstream>
#include <iostream>
#include <string>

class Log
{
  private:
    std::array<char, 2048> mem{};
    int offset{};
    std::string dir_path = "data/mem.txt";

    void readFromDisk(const std::string &path);

  public:
    Log();
    int append(const std::string &message);
    std::string read(int givenOffset);
};
