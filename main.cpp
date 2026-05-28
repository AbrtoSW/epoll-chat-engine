#include <iostream>

#include "reactor_engine.h"

int main() {

   
    ReactorEngine e;

    
    if (!e.init()) {
        return -1;
    }

    std::cout << "Server initialized, entering reactor loop..." << std::endl;

    e.run();



    return 0;
}