#pragma once

#include <cstdint>
#include <string>
//#include <sstream>

#include "../exceptions/exceptor.h"
#include "../system/epoller.h"
#include "../transfer_conveyer.h"
#include "../synchronization/signal_pack.h"

namespace receiver_nms {

const unsigned buffer_size = 8192;

using std::string;

template <class ConveyerSide> class receiver_t : public exceptor_t, protected epoller_t<128> {
    transfer_conveyer_t *conveyer_;
    signal_pack_t *data_signal_;
    int8_t buf_[buffer_size];

    unsigned receive(const string &dsc, int sock_fd, const int8_t **data, int *transfer_flag) {
        int bytes_read = read(message_on_error, sock_fd, buf_, sizeof buf_);
        //show_message(("from socket: " + std::to_string(sock_fd) + " received bytes: " + std::to_string(bytes_read)).c_str());
        if(0 < bytes_read){
            *transfer_flag = data_pending;
            *data = buf_;
            return bytes_read;
        }
        if(bytes_read == 0){
            *transfer_flag = descriptor_shutdown;
            return 0;
        }
        if(errno == EWOULDBLOCK || errno == EAGAIN){
            epoll_event ee;
            ee.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
            ee.data.fd = sock_fd;
            int res = epoll_ctl(message_on_error, epoll_fd(), EPOLL_CTL_MOD, sock_fd, &ee);
            if(res != -1){
                //std::stringstream ss;
                //ss << std::this_thread::get_id();
                //show_message(("registered socket: " + std::to_string(sock_fd) + " pid: " + ss.str()).c_str());
                *transfer_flag = no_transfer_flag;
                return 0;
            }
        }
        show_message_except("unable to receive data", [&](){
            return dsc + " : unable to receive data";
        });
        *transfer_flag = descriptor_error;
        return 0;
    }

public:

    receiver_t(signal_pack_t *data_signal, int epoll_fd, task_control_t *ctrl, resource_waiter_t *memory_waiter
               , transfer_conveyer_t *conveyer, const std::function<void (const char *)> &message_f)
        : exceptor_t(memory_waiter, ctrl, message_f)
        , epoller_t(epoll_fd, max_response, message_f, [this](operation_t op){ return except(op); })
        , conveyer_(conveyer), data_signal_(data_signal) {}

    const char *name() const override { return "receiver"; }

protected:

    bool one_step() override {
        const auto message_f = [this](const char *default_msg
                                      , const std::function<string ()> &full_msg){
            show_message_except(default_msg, full_msg);
        };
        const auto except_f = [this](operation_t op){ return except(op); };
        const auto receive_f = [this](const string &dsc, int sock, const int8_t **data
                , int *transfer_flag){
            return receive(dsc, sock, data, transfer_flag);
        };
        unsigned cnt = conveyer_->write<ConveyerSide>(this, message_f, except_f, receive_f
                                                      , [](int transfer_flag){
            return transfer_flag == data_pending;
        });
        data_signal_->notify_n(cnt);
        return epoll([this, message_f, except_f, receive_f](int sock){
                if(conveyer_->write(this, sock, message_f, except_f, receive_f)){
                    data_signal_->notify_one();
                }
            }, [this](int){ show_message("out of band data ignored"); }, !cnt
        ) != - 1;
    }

    bool on_start() override {
        show_message("receiver thread started");
        return true;
    }

    void on_finish() override {
        show_message("receiver thread finished");
    }
};

}

using receiver_nms::receiver_t;
