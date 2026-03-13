#include "log.h"
#include <iostream>

int main()
{
    Log log{};
    std::cout << log.append("testing") << std::endl;
    std::cout << log.append("justin") << std::endl;
    std::cout << "TEST: " << log.read(1) << std::endl;
}
