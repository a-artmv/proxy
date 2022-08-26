#pragma once

#include <fstream>
#include <string>
#include <chrono>
#include <iomanip>
#include <sstream>

#include "reader.h"

namespace logger_nms {

const unsigned lane_num = 1;
const char *log_file = "log";

using std::ofstream;
using std::string;
using sys_clock_t = std::chrono::system_clock;
using sys_time_t = std::chrono::time_point<sys_clock_t>;
using std::chrono::duration_cast;
using std::chrono::milliseconds;

template <class ConveyerSide> class logger_t : public reader_t<ConveyerSide, lane_num> {
    typedef reader_t<ConveyerSide, lane_num> Base;
    using Base::num;
    using Base::except;
    using Base::show_message;
    using Base::show_message_except;

    sys_time_t start;

    virtual const char *file_name() { return "to_clients_"; }

    virtual const char *message() {
        return "! This is server side logging, for SQL queries see clients side !\n"
                "-----------------------\n";
    }

protected:

    ofstream log_;

    string time_stamp() {
        unsigned msecs = duration_cast<milliseconds>(sys_clock_t::now() - start).count();
        unsigned secs = msecs / 1000;
        msecs = msecs - secs * 1000;
        unsigned mins = secs / 60;
        secs = secs - mins * 60;
        unsigned hours = mins / 60;
        mins = mins - hours * 60;
        unsigned days = hours / 24;
        hours = hours - days * 24;
        std::ostringstream ss;
        ss << days <<'d' << hours << 'h' << mins << 'm' << secs << 's' << msecs << "ms";
        return ss.str();
    }

public:

    logger_t(unsigned reader_num, signal_pack_t *data_signal, task_control_t *ctrl
             , resource_waiter_t *memory_waiter, transfer_conveyer_t *conveyer
             , const std::function<void (const char *)> &message_f)
        : Base(reader_num, data_signal, ctrl, memory_waiter, conveyer, message_f) {
        log_.exceptions(std::ofstream::failbit | std::ofstream::badbit);
    }

    const char *name() const override { return "logger"; }

protected:

    bool on_start() override {
        string file_nm;
        if(except([&](){ file_nm
                  = std::string(file_name()) + log_file + std::to_string(num()); })){
            if(except([&](){ log_.open(file_nm, std::ios::app); })){
                show_message_except("logger thread started", [&](){
                    return "logger thread started, logging to: " + file_nm;
                });
                start = sys_clock_t::now();
                auto time = std::chrono::system_clock::to_time_t(start);
                except([&](){
                    log_ << "-----------------------\n";
                    log_ << message();
                    log_ << "    logging started\n";
                    log_ << std::put_time(std::localtime(&time), "%c %Z \n");
                    log_ << "-----------------------\n";
                });
                return true;
            }
        }
        show_message("can't start logger thread");
        return false;
    }

    void on_finish() override {
        except([&](){ log_.close(); });
        show_message("logger thread finished");
    }

    unsigned read_data(const string &dsc, int, page_wrapper_t &&wpage, int *) override {
        except([&](){
            log_ << '(' << time_stamp() << ") "
                 << dsc << " : " << wpage.size() << " bytes transferred\n";
        });
        return wpage.size();
    }

    bool read_if(int transfer_flag) override { return transfer_flag == no_transfer_flag; }
};

}

using logger_nms::logger_t;
