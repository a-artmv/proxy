#pragma once

#include "../exceptions/exceptor.h"
#include "../system/sys_caller.h"
#include "../transfer_conveyer.h"
#include "../synchronization/signal_pack.h"

namespace reader_nms {

template <class ConveyerSide, unsigned LANE_NUM>
class reader_t : public exceptor_t, protected sys_caller_t {
    signal_pack_t *data_signal_;
    transfer_conveyer_t *conveyer_;
    const unsigned reader_num_;

protected:

    reader_t(unsigned reader_num, signal_pack_t *data_signal, task_control_t *ctrl
             , resource_waiter_t *memory_waiter, transfer_conveyer_t *conveyer
             , const std::function<void (const char *)> &message_f)
        : exceptor_t(memory_waiter, ctrl, message_f)
        , sys_caller_t(message_f, [this](operation_t op){ return except(op); })
        , data_signal_(data_signal), conveyer_(conveyer), reader_num_(reader_num) {}

    transfer_conveyer_t *conveyer() const { return conveyer_; }

    unsigned num() const { return reader_num_; }

    bool one_step() override {
        const auto message_f = [this](const char *default_msg
                                      , const std::function<std::string ()> &full_msg_f){
            show_message_except(default_msg, full_msg_f);
        };
        const auto except_f = [this](operation_t op){ return except(op); };
        const auto read_f = [this](const std::string &dsc, int sock
                , page_wrapper_t &&wpage, int *transfer_flag){
            return read_data(dsc, sock, std::move(wpage), transfer_flag);
        };
        if(conveyer_->read<ConveyerSide>(this, LANE_NUM, message_f, except_f, read_f
                , [this](int transfer_flag){ return read_if(transfer_flag); })){
            return true;
        }
        const auto cond_f = [&](){
            return conveyer_->ready_read<ConveyerSide>(this, LANE_NUM, message_f, except_f
                        , [this](int transfer_flag){ return read_if(transfer_flag); })
                    <= reader_num_;
        };
        return data_signal_->wait(LANE_NUM, max_response, [this](){ return stop_flag(); }, cond_f);
    }

    virtual unsigned read_data(const std::string &description, int descriptor
                               , page_wrapper_t &&wpage, int *transfer_flag) = 0;

    virtual bool read_if(int transfer_flag) = 0;
};

}

using reader_nms::reader_t;
