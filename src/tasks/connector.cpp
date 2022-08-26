#include <arpa/inet.h>
#include <string>
#include <thread>
#include <memory>
#include <utility>

#include "connector.h"
#include "../transfer_conveyer.h"
#include "../exceptions/destructoid.h"

using std::string;
using std::unique_ptr;

class connected_peer_t : public peer_t {
    friend class connector_t;
    destructoid_t disconnector_;

public:

    connected_peer_t(int client_sock, int server_sock) : peer_t(client_sock, server_sock) {}
};

void connector_t::add_peer(int sock) {
    sockaddr_in client_addr;
    socklen_t addr_l = sizeof(sockaddr_in);
    int client_sock = accept(message_on_error, sock, (sockaddr *)&client_addr, &addr_l
                             , SOCK_NONBLOCK);
    if(client_sock == -1){
        return;
    }
    destructoid_t cleaner;
    if(!prepend_or_call(&cleaner, [client_sock, this](){
                        shutdown(message_on_error, client_sock, SHUT_RDWR);
                        close(message_on_error, client_sock);
                        show_message("peer dropped"); })){
        return;
    }
    int server_sock = socket(message_on_error, PF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);
    if(server_sock == -1){
        return;
    }
    if(!prepend_or_call(&cleaner, [server_sock, this](){ close(message_on_error, server_sock); })){
        return;
    }
    for(int i = 0; ; ++i){
        int res = connect(message_on_error, server_sock, (sockaddr *)&server_addr_
                          , sizeof(sockaddr_in));
        if(res == -1){
            int error = errno;
            if(error == EINPROGRESS){
                int epoll_fd = epoll_create(message_on_error);
                if(epoll_fd == -1){
                    return;
                }
                destructoid_t epoll_cleaner;
                if(!prepend_or_call(&epoll_cleaner, [epoll_fd, this](){
                                    close(message_on_error, epoll_fd); })){
                    return;
                }
                epoll_event ee;
                ee.events = EPOLLOUT;
                ee.data.fd = server_sock;
                res = epoll_ctl(message_on_error, epoll_fd, EPOLL_CTL_ADD, server_sock, &ee);
                if(res == -1){
                    return;
                }
                if(!prepend_or_call(&epoll_cleaner, [epoll_fd, server_sock, this]{
                                    epoll_ctl(message_on_error, epoll_fd, EPOLL_CTL_DEL, server_sock
                                              , nullptr); })){
                    return;
                }
                {
                    unique_ptr<epoller_t<1>> epoller;
                    if(!except([&](){ epoller = std::make_unique<epoller_t<1>>(epoll_fd, max_response
                                            , [this](const char *m){ show_message(m); }
                                            , [this](operation_t op){ return except(op); }); })){
                        return;
                    }
                    while((res = epoller->epoll([](int){})) == 0){
                        if(stop_flag()){
                            return;
                        }
                    }
                    if(res == -1){
                        return;
                    }
                }
                socklen_t len = sizeof(int);
                res = getsockopt(message_on_error, server_sock, SOL_SOCKET, SO_ERROR, &error, &len);
                if(res == -1){
                    return;
                }
                if(!error){
                    break;
                }
                if(error != EAGAIN){
                    message_error("connect", error);
                    return;
                }
            }
            if(error == EAGAIN){
                show_message("connect: insufficient entries in the routing cache");
                if(i < 5){
                    std::this_thread::sleep_for(max_response);
                    if(stop_flag()){
                        return;
                    }
                    continue;
                }
            }
            return;
        }
        break;
    }
    if(!prepend_or_call(&cleaner, [server_sock, this](){
                        shutdown(message_on_error, server_sock, SHUT_RDWR); })){
        return;
    }
    unique_ptr<connected_peer_t> peer;
    if(!except([&](){ peer = std::make_unique<connected_peer_t>(client_sock, server_sock); })){
        return;
    }
    string client_name;
    if(!except([&](){ client_name = string(inet_ntoa(client_addr.sin_addr))+ ":"
                                                + std::to_string(client_addr.sin_port); })){
        return;
    }
    auto *peer_disconnector = &peer->disconnector_;
    if(!except([&](){ conveyer_->add_peer(client_name, std::move(peer)); })){
        return;
    }
    const auto register_epoll = [&cleaner, this](int epoll_fd, int socket_fd, uint32_t events){
        epoll_event ee;
        ee.events = events;
        ee.data.fd = socket_fd;
        int res = epoll_ctl(message_on_error, epoll_fd, EPOLL_CTL_ADD, socket_fd, &ee);
        if(res == -1){
            return false;
        }
        if(!prepend_or_call(&cleaner, [epoll_fd, socket_fd, this](){
                            epoll_ctl(message_on_error, epoll_fd, EPOLL_CTL_DEL, socket_fd
                                      , nullptr); })){
            return false;
        }
        return true;
    };
    register_epoll(clients_receivers_epoll_, client_sock, EPOLLIN | EPOLLET | EPOLLONESHOT);
    register_epoll(clients_senders_epoll_, client_sock, EPOLLOUT | EPOLLET);
    register_epoll(server_receivers_epoll_, server_sock, EPOLLIN | EPOLLET | EPOLLONESHOT);
    register_epoll(server_senders_epoll_, server_sock, EPOLLOUT | EPOLLET);
    string msg;
    except([&](){ msg = client_name + " : disconnecting peer"; });
    except([&](){ cleaner.prepend([this, m = std::move(msg)](){ show_message(m.c_str()); }); });
    peer_disconnector->swap(cleaner);
}
