#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <cstring>
#include <string>
#include <iostream>
#include <memory>
#include <thread>
#include <atomic>
#include <mutex>

#include "proxy.h"

namespace console_nms {

using std::cout;
using std::cin;
using std::unique_ptr;

inline void show_message(const char *message) {
    static std::mutex mx;
    std::lock_guard<std::mutex> lg(mx);
    cout << message << std::endl;
}

class console_t {
    unique_ptr<proxy_t> proxy_;
    signal_t error_signal_;
    bool args_ok_;

public:

    console_t(int argc, char *argv[]) {
        uint16_t proxy_port = 54321;
        uint16_t srv_port = 5432;
        in_addr srv_host;
        inet_aton("127.0.0.1", &srv_host);
        bool no_opts = argc < 2;
        args_ok_ = argc % 2 && !no_opts;
        const auto get_port = [](uint16_t *dest, const char *src){
            int port = std::atoi(src);
            bool ok = 0 < port && port < 65536;
            if(ok){
                *dest = static_cast<uint16_t>(port);
            }
            return ok;
        };
        const char *arg = nullptr;
        for(int i = 1; i < argc && args_ok_; ++i){
            arg = argv[i++];
            if(std::strcmp(arg, "-p") == 0){
                args_ok_ = get_port(&proxy_port, argv[i]);
            } else if(std::strcmp(arg, "-sh") == 0){
                auto host = gethostbyname(argv[i]);
                if((args_ok_ = host)){
                    srv_host.s_addr = *reinterpret_cast<uint32_t *>(host->h_addr);
                }
            } else if(std::strcmp(arg, "-sp") == 0){
                args_ok_ = get_port(&srv_port, argv[i]);
            } else{
                args_ok_ = false;
            }
        }
        if(!args_ok_){
            if(no_opts){
                //cout << "no command line options supplied\n";
            } else{
                std::string msg = "bad command line option: \"";
                if(arg){
                    msg += arg;
                }
                msg += "\"\n";
                cout << msg;
            }
            cout << "usage: proxy -p <listening_port> "
                         "-sh <server_host> -sp <server_port>\n";
        }
        cout << "current parameters:"
                  << "\nproxy listening port: " << proxy_port
                  << "\npostgres server host: " << inet_ntoa(srv_host)
                  << "\npostgres server port: " << srv_port << std::endl;
        proxy_port = htons(proxy_port);
        srv_port = htons(srv_port);
        proxy_ = std::make_unique<proxy_t>(&error_signal_, show_message
                                           , htonl(INADDR_ANY), proxy_port
                                           , srv_host.s_addr, srv_port);
    }

    int exec() {
        bool auto_start = args_ok_;
        while(cin){
            std::string input;
            if(auto_start){
                auto_start = false;
                input = "s";
            } else{
                cout << "enter \"q\" to quit\n"
                        "enter \"s\" to start proxy "
                        "(press \"Return\" to stop it)\n";
                cin >> input;
            }
            if(input == "q"){
                return 0;
            } else if(input == "s"){
                std::atomic_int user_interrupt = { 0 };
                std::thread user_thread;
                try{
                    user_thread = std::thread([&user_interrupt, e_signal = &error_signal_](){
                        try{
                            cin.ignore();
                            cin.get();
                            user_interrupt.store(1, std::memory_order_release);
                            e_signal->notify_all();
                        } catch(const std::exception &e){
                            show_message("exception in the console input thread: ");
                            show_message(e.what());
                        } catch(...){
                            show_message("unknown exception in the console input thread");
                        }
                    });
                    proxy_->start();
                    error_signal_.wait();
                } catch(const std::exception &e){
                    cout << "exception: " << e.what() << std::endl;
                }
                if(!user_interrupt.load(std::memory_order_acquire)){
                    cin.putback('\n');
                }
                user_thread.join();
                proxy_->stop();
                error_signal_.reset();
                continue;
            } else{
                continue;
            }
        }
        return 1;
    }
};

}

using console_nms::console_t;
