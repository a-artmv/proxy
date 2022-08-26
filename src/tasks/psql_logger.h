#pragma once

#include "logger.h"
#include"../postgre_msg.h"

namespace psql_logger_nms {

using std::string;
using std::ofstream;

template <class ConveyerSide> class psql_logger_t : public logger_t<ConveyerSide> {
    static inline postgre_msg_t pmsg_;
    typedef logger_t<ConveyerSide> Base;

    const char *file_name() override { return "from_clients_"; }

    const char *message() override { return ""; }

public:

    psql_logger_t(unsigned reader_num, signal_pack_t *data_signal, task_control_t *ctrl
                  , resource_waiter_t *memory_waiter, transfer_conveyer_t *conveyer
                  , const std::function<void (const char *)> &message_f)
        : Base(reader_num, data_signal, ctrl, memory_waiter, conveyer, message_f) {}

    unsigned read_data(const string &dsc, int sock
                       , page_wrapper_t &&wpage, int *) override {
        auto sz = wpage.size();
        const auto except_f = [this](operation_t op){ return exceptor_t::except(op); };
        exceptor_t::except([&](){
            string preamble = "(" + Base::time_stamp() + ") " + dsc + " : ";
            pmsg_.add_data(sock, std::move(wpage), preamble, Base::log_, except_f);
        });
        return sz;
    }

    static void reset() { pmsg_.reset(); }

    static void clear_from(int sock) { pmsg_.clear_from(sock); }
};

}

using psql_logger_nms::psql_logger_t;
