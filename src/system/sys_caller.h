#pragma once

#include <errno.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <cassert>

#include "base_sys_caller.h"

namespace sys_caller_nms {

class sys_caller_t : protected base_sys_caller_t {
public:

    sys_caller_t(const std::function<void (const char *)> &message_f
                 , const std::function<bool (operation_t)> &except_f = no_catch_f)
        : base_sys_caller_t(message_f, [](){ return errno; }
                            , { EAGAIN, EWOULDBLOCK }, { EINTR }, except_f) {}

    template <class OnError> int epoll_create(OnError a) {
        return call(a, "create epoll descriptor", [](){ return ::epoll_create1(0); });
    }

    template <class OnError>
    int epoll_wait(OnError a, int fd, epoll_event *events, int max_events, int timeout) {
        return call(a, "epoll_wait", [=](){ return ::epoll_wait(fd, events, max_events, timeout); });
    }

    template <class OnError> int epoll_ctl(OnError a, int epfd, int op, int fd, epoll_event *event) {
        return call(a, "epoll_ctl", [=](){ return ::epoll_ctl(epfd, op, fd, event); });
    }

    template <class OnError> int close(OnError a, int fd) {
        return call(a, "close descriptor", [fd](){ return ::close(fd); }, call_once);
    }

    template <class OnError> int socket(OnError a, int domain, int type, int protocol) {
        return call(a, "create socket descriptor", [=](){ return ::socket(domain, type, protocol); });
    }

    template <class OnError>
    int bind(OnError a, int sockfd, const sockaddr *addr, socklen_t addrlen) {
        return call(a, "bind", [=](){ return ::bind(sockfd, addr, addrlen); });
    }

    template <class OnError> int listen(OnError a, int sockfd, int backlog) {
        return call(a, "listen", [=](){ return ::listen(sockfd, backlog); });
    }

    template <class OnError>
    int accept(OnError a, int sockfd, sockaddr *addr, socklen_t *addrlen, int flags = 0) {
        return call(a, "accept", [=](){ return ::accept4(sockfd, addr, addrlen, flags); });
    }

    template <class OnError> int shutdown(OnError a, int sockfd, int how) {
        return call(a, "shutdown socket", [=](){ return ::shutdown(sockfd, how); });
    }

    template <class OnError>
    int connect(OnError a, int sockfd, const sockaddr *addr, socklen_t addrlen) {
        return call(a, "connect", [=](){ return ::connect(sockfd, addr, addrlen); });
    }

    template <class OnError>
    int getsockopt(OnError a, int sockfd, int level, int optname, void *optval, socklen_t *optlen) {
        return call(a, "getsockopt", [=](){
            return ::getsockopt(sockfd, level, optname, optval, optlen);
        });
    }
    template <class OnError>
    int write(OnError a, int fd, void *buf, size_t count) {
        return call(a, "write", [=](){ return ::write(fd, buf, count); });
    }

    template <class OnError>
    int read(OnError a, int fd, void *buf, size_t count) {
        return call(a, "read", [=](){ return ::read(fd, buf, count); });
    }
};

}

using sys_caller_nms::sys_caller_t;
