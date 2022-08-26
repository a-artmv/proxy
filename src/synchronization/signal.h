#pragma once

#include <mutex>
#include <condition_variable>
#include <atomic>

namespace signal_nms {

using std::mutex;
using std::condition_variable;
using std::atomic_int;
using std::unique_lock;
using std::chrono::duration;

constexpr auto always_true = [](){ return true; };
constexpr auto always_false = [](){ return false; };

class signal_t {
    mutex mx_;
    condition_variable vr_;
    atomic_int blocked_cnt_ = { 0 };
    int notify_cnt_ = 0;
    unsigned threading_lvl_ = 0;

    template <class Condition, class CntOp, class NotifyOp>
    void do_notify(Condition condition, CntOp cnt_op, NotifyOp notify_op) {
        if(0 < blocked_cnt_.load(std::memory_order_acquire)){
            bool notify = false;
            mx_.lock();
            if(0 < blocked_cnt_.load(std::memory_order_relaxed)){
                if(condition()){
                    cnt_op();
                    notify = true;
                }
            }
            mx_.unlock();
            if(notify){
                notify_op();
            }
        }
    }

    template <class Condition, class StopCondition, class WaitOp>
    bool do_wait(Condition condition, StopCondition stop, WaitOp wait_op) {
        unique_lock<mutex> ul(mx_);
        if(!condition()){
            return true;
        }
        blocked_cnt_.fetch_add(1, std::memory_order_relaxed);
        notify_cnt_ = blocked_cnt_.load(std::memory_order_relaxed);
        bool ok = true;
        while(blocked_cnt_.load(std::memory_order_relaxed) == notify_cnt_){
            wait_op(ul);
            if(stop()){
                ok = false;
                break;
            }
        }
        blocked_cnt_.fetch_sub(1, std::memory_order_relaxed);
        return ok;
    }

public:

    signal_t() {};

    ~signal_t() { notify_all(); }

    void reset() {
        blocked_cnt_.store(0, std::memory_order_relaxed);
        notify_cnt_ = 0;
    }

    template <class Condition = decltype(always_true)>
    void notify_all(Condition condition = always_true) {
        do_notify(condition, [this](){ notify_cnt_ = 0; }
                  , [this](){ vr_.notify_all(); });
    }

    template <class Condition = decltype(always_true)>
    void notify_one(Condition condition = always_true) {
        do_notify(condition, [this](){ notify_cnt_ -= 1; }, [this](){ vr_.notify_one(); });
    }

    void set_threading_level(unsigned cnt) { threading_lvl_ = cnt; }

    template <class Condition = decltype(always_true)>
    void notify_n(unsigned n, Condition condition = always_true) {
        if(n < threading_lvl_){
            do_notify(condition, [this, n](){ notify_cnt_ -= n; }
                      , [this, n](){ for(unsigned i = 0; i < n; ++i){ vr_.notify_one(); } });
        } else{
            notify_all(condition);
        }
    }

    template <class Condition = decltype(always_true)>
    bool wait(Condition condition = always_true) {
        return do_wait(condition, always_false, [this](unique_lock<mutex> &ul){ vr_.wait(ul); });
    }

    template <class R, class P, class StopCondition, class Condition = decltype(always_true)>
    bool wait(const duration<R, P> &period, StopCondition stop
                 , Condition condition = always_true) {
       return do_wait(condition, stop
                      , [this, &period](unique_lock<mutex> &ul){ vr_.wait_for(ul, period); });
    }
};

}

using signal_nms::signal_t;
