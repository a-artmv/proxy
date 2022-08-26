#pragma once

#include <vector>
#include <cassert>

#include "signal.h"

namespace signal_pack_nms {

using std::vector;

class signal_pack_t {
    vector<signal_t> signals_;

public:

    signal_pack_t(unsigned waiters_cnt) : signals_(waiters_cnt) {}

    template <class... T> bool wait(unsigned waiter_idx, T... a) {
        assert(waiter_idx < signals_.size());
        return signals_[waiter_idx].wait(a...);
    }

    template <class... T> void notify_one(T... a) {
        for(auto &signal : signals_){
            signal.notify_one(a...);
        }
    }

    template <class... T> void notify_all(T... a) {
        for(auto &signal : signals_){
            signal.notify_all(a...);
        }
    }

    template <class... T> void notify_n(T... a) {
        for(auto &signal : signals_){
            signal.notify_n(a...);
        }
    }

    template <class... T> void notify_one_waiter(unsigned waiter_idx, T... a) {
        assert(waiter_idx < signals_.size());
        signals_[waiter_idx].notify_one(a...);
    }

    template <class... T> void notify_all_waiters(unsigned waiter_idx, T... a) {
        assert(waiter_idx < signals_.size());
        signals_[waiter_idx].notify_all(a...);
    }

    template <class... T> void notify_n_waiters(unsigned waiter_idx, T... a) {
        assert(waiter_idx < signals_.size());
        signals_[waiter_idx].notify_n(a...);
    }

    void set_threading_level(unsigned cnt) {
        for(auto &signal : signals_){
            signal.set_threading_level(cnt);
        }
    }

    void set_waiters_threading_level(unsigned waiter_idx, unsigned cnt) {
        assert(waiter_idx < signals_.size());
        signals_[waiter_idx].set_threading_level(cnt);
    }

    void reset() {
        for(auto &signal : signals_){
            signal.reset();
        }
    }
};

}

using signal_pack_nms::signal_pack_t;
