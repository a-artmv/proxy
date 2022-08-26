#include <iostream>

#include "console.h"

int main(int argc, char *argv[]) {
    int r;
    try{
        r = console_t(argc, argv).exec();
    }  catch(const std::exception &e){
        std::cout << "fatal error: " << e.what() << std::endl;
        r = 2;
    } catch(...){
        std::cout << "unknown fatal error\n";
        r = 2;
    }
    return r;
}
