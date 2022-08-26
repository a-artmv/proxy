#pragma once

#include "reader.h"
#include "../system/epoller.h"

namespace sender_nms {

const unsigned lane_num = 0;

template <class ConveyerSide> class senders_helper_t : public exceptor_t, protected epoller_t<1024> {
    transfer_conveyer_t *conveyer_;
    signal_pack_t *data_signal_;

public:

    senders_helper_t(signal_pack_t *data_signal, int epoll_fd, task_control_t *ctrl
                     , resource_waiter_t *memory_waiter, transfer_conveyer_t *conveyer
                     , const std::function<void (const char *)> &message_f)
        : exceptor_t(memory_waiter, ctrl, message_f)
        , epoller_t(epoll_fd, max_response, message_f, [this](operation_t op){ return except(op); })
        , conveyer_(conveyer), data_signal_(data_signal) {}

    const char *name() const override { return "senders helper"; }

protected:

    bool one_step() override {
        const auto flag_f = [](int *transfer_flag){
            if(*transfer_flag == data_pending){
                *transfer_flag = no_transfer_flag;
                return true;
            }
            return false;
        };
        return epoll([&](int sock){
                if(conveyer_->flag(this, sock, lane_num, flag_f))
                    { data_signal_->notify_one_waiter(lane_num); }
               }) != -1;
    }

    bool on_start() override {
        show_message("senders helper thread started");
        return true;
    }

    void on_finish() override {
        show_message("senders helper thread finished");
    }
};

template <class ConveyerSide> class sender_t : public reader_t<ConveyerSide, lane_num> {
    typedef reader_t<ConveyerSide, lane_num> Base;
    using Base::show_message;
    using Base::show_message_except;
    using Base::conveyer;
    using Base::write;
    using Base::message_on_error;

public:

    sender_t(unsigned reader_num, signal_pack_t *data_signal, task_control_t *ctrl
             , resource_waiter_t *memory_waiter, transfer_conveyer_t *conveyer
             , const std::function<void (const char *)> &message_f)
        : Base(reader_num, data_signal, ctrl, memory_waiter, conveyer, message_f) {}

    const char *name() const override { return "sender"; }

protected:

    bool on_start() override {
        show_message("sender thread started");
        return true;
    }

    void on_finish() override {
        show_message("sender thread finished");
    }

    unsigned read_data(const std::string &dsc, int sock
                       , page_wrapper_t &&wpage, int *transfer_flag) override {
        int dest_sock = conveyer()->other_side(sock);
        unsigned bytes_send = 0;
        while(bytes_send != wpage.size()){
            int res = write(message_on_error, dest_sock
                            , (void *)(wpage.data() + bytes_send), wpage.size() - bytes_send);
            //show_message(("to socket: " + std::to_string(dest_sock) + " sent bytes: " + std::to_string(res)).c_str());
            if(res == -1){
                if(errno == EAGAIN || errno == EWOULDBLOCK){
                    *transfer_flag =  data_pending;
                } else{
                    *transfer_flag =  descriptor_error;
                    show_message_except("unable to send data", [&](){
                        return dsc + " : unable to send data";
                    });
                }
                break;
            }
            if(res == 0){
                *transfer_flag = descriptor_shutdown;
                break;
            }
            bytes_send += res;
        }
        return bytes_send;
    }

    bool read_if(int transfer_flag) override { return transfer_flag == no_transfer_flag; }
};

}

using sender_nms::sender_t;
using sender_nms::senders_helper_t;
