#pragma once

#include "transfer_conveyer.h"

#include <string>
#include <fstream>
#include <queue>
#include <unordered_map>
#include <mutex>
#include <functional>

namespace postgre_msg_nms {

using std::ofstream;
using std::string;
using std::queue;
using std::unordered_map;
using std::mutex;
using std::lock_guard;
using std::function;

typedef function<void (const function<void ()> &)> ExceptF;

class protocol_message_t {
    queue<page_wrapper_t> data_;
    int cur_size_;
    int state_;
    int type_;
    int size_;
    int8_t size_bytes_[4];
    char type_byte_;

    void throw_error(ofstream &log, const ExceptF &except_f);

    void process(page_wrapper_t &&wpage, const string &preamble, ofstream &log
                 , const ExceptF &except_f);

    bool check_type();

public:

    protocol_message_t();

    void add_data(page_wrapper_t &&wpage, const string &preamble, ofstream &log
                  , const ExceptF &except_f);
};

class postgre_msg_t {
    unordered_map<int, protocol_message_t> msg_;
    mutex mx_;

public:

    void reset() { msg_.clear(); }

    void clear_from(int sock) {
        lock_guard<mutex> lg(mx_);
        auto it = msg_.find(sock);
        if(it != msg_.end()){
            msg_.erase(it);
        }
    }

    void add_data(int sock, page_wrapper_t &&wpage, const string &preamble, ofstream &log
                  , const ExceptF &except_f) {
        mx_.lock();
        protocol_message_t &m = msg_[sock];
        mx_.unlock();
        m.add_data(std::move(wpage), preamble, log, except_f);
    }
};

}

using postgre_msg_nms::postgre_msg_t;
