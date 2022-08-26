#pragma once

#include <sys/epoll.h>
#include <chrono>
#include <memory>

#include "sys_caller.h"

namespace epoller_nms {

using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::duration;
using std::unique_ptr;

constexpr auto empty_op = [](int){};

template <unsigned MAX_EVENTS> class epoller_t : protected sys_caller_t {
    const int fd_;
    epoll_event events_[MAX_EVENTS];
    const int timeout_;

    epoller_t(const epoller_t &) = delete;
    epoller_t &operator=(const epoller_t &) = delete;
    epoller_t(const epoller_t &&) = delete;
    epoller_t &operator=(const epoller_t &&) = delete;

public:

    template <class R, class P>
    epoller_t(int fd, const duration<R, P> &timeout
              , const std::function<void (const char *)> &message_f
              , const std::function<bool (operation_t)> &except_f)
        : sys_caller_t(message_f, except_f)
        , fd_(fd), timeout_(duration_cast<milliseconds>(timeout).count()) {}

    template <class O1, class O2 = decltype(empty_op)>
    int epoll(const O1 &io_operation, const O2 &on_oob = empty_op, bool wait = true) {
        int cnt = epoll_wait(message_on_error, fd_, events_, MAX_EVENTS, wait ? timeout_ : 0);
        for(int i = 0; i < cnt; ++i){
            const unsigned events = events_[i].events;
            const int fd = events_[i].data.fd;
            if(events & EPOLLPRI){ on_oob(fd); }
            if(events & EPOLLIN || events & EPOLLOUT || events & EPOLLHUP || events & EPOLLERR){
                io_operation(fd);
            }
        }
        return cnt;
    }

    int epoll_fd() const { return fd_; }
};

}

using epoller_nms::epoller_t;
