#include "log.h"
#include <iostream>

int main()
{
    Log log{};
    std::cout << log.append("test") << std::endl;
    std::cout << log.append("blah") << std::endl;
    std::cout << "Test: " << log.read(6) << std::endl;
}
