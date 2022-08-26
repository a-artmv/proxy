#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <chrono>

namespace task_nms {

using namespace std::chrono_literals;
using std::atomic_uint;
using std::atomic_int;
using std::condition_variable;
using std::mutex;
using std::unique_lock;

constexpr auto max_response = 500ms;

class task_t;

class task_control_t {
    atomic_uint stop_flag_ = { 0 };
    atomic_uint pause_flag_ = { 0 };
    condition_variable resume_cv_;
    mutex resume_mx_;

public:

    task_control_t() {}

    void stop() { stop_flag_.store(1, std::memory_order_release); }

    void pause() { pause_flag_.store(1, std::memory_order_release); }

    void resume() {
        if(pause_flag()){
            bool notify = false;
            resume_mx_.lock();
            if(pause_flag()){
                pause_flag_.store(0);
                notify = true;
            }
            resume_mx_.unlock();
            if(notify){
                resume_cv_.notify_all();
            }
        }
    }

    bool stop_flag() const { return stop_flag_.load(std::memory_order_acquire); }

    bool pause_flag() const { return pause_flag_.load(std::memory_order_acquire); }

    void reset() {
        stop_flag_.store(0, std::memory_order_release);
        pause_flag_.store(0, std::memory_order_release);
    }

private:

    friend class task_t;

    bool tick() {
        if(stop_flag()){
            return false;
        }
        if(pause_flag()){
            unique_lock<mutex> ul(resume_mx_);
            while(pause_flag()){
                resume_cv_.wait_for(ul, max_response);
                if(stop_flag()){
                    return false;
                }
            }
        }
        return true;
    }
};

class task_t {
    task_control_t *tctrl_;
    atomic_int utility_flag_ = { 0 };
    atomic_uint yield_flag_ = { 0 };

public:

    virtual ~task_t() = default;

    virtual const char *name() const { return ""; }

    bool stop_flag() const { return tctrl_->stop_flag(); }

    bool pause_flag() const { return tctrl_->pause_flag(); }

    int utility_flag() const { return utility_flag_.load(std::memory_order_acquire); }

    void set_utility_flag(int val) { utility_flag_.store(val, std::memory_order_release); }

    bool is_yielding() {
        bool yielding = yield_flag_.load(std::memory_order_acquire);
        if(yielding){
            yield_flag_.store(0, std::memory_order_release);
        }
        return yielding;
    }

    void yield() { yield_flag_.store(1, std::memory_order_release); }

    void run() {
        if(on_start()){
            for(;;){
                if(tctrl_->tick()){
                    if(one_step()){
                        continue;
                    }
                }
                break;
            }
        }
        on_finish();
        yield_flag_.store(0, std::memory_order_release);
        utility_flag_.store(0, std::memory_order_release);
    }

protected:

    task_t(task_control_t *ctrl) : tctrl_(ctrl) {}

    virtual bool one_step() = 0;

    virtual bool on_start() { return true; }

    virtual void on_finish() {}
};

template <int ACQUIRE_VAL, int RELEASE_VAL> class utility_flag_helper {
    task_t *task_;

public:

    utility_flag_helper(task_t *task) : task_(task) {
        task_->set_utility_flag(ACQUIRE_VAL);
    }

    ~utility_flag_helper() {
        task_->set_utility_flag(RELEASE_VAL);
    }
};

namespace utility_flags {

const int no_utility_flag = 0;
const int task_blocked = 1;

}

using namespace utility_flags;

}

using task_nms::max_response;
using task_nms::task_control_t;
using task_nms::task_t;
using task_nms::utility_flag_helper;
using task_nms::no_utility_flag;
using task_nms::task_blocked;
