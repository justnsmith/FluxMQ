#include "log.h"
#include <iostream>

int main() {
    Log log{};
    std::cout << log.append("test") << std::endl;
}
