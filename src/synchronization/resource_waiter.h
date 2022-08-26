#pragma once

#include <string>
#include <functional>
#include <utility>
#include <atomic>

#include "signal.h"
#include "../tasks/task.h"

namespace waiter_nms {

using std::function;
using std::atomic_int;

class resource_waiter_t {
    signal_t signal_;
    int counter_ = 0;
    const int required_;
    function<void (task_t *)> on_block_;
    function<void (void)> on_release_;

public:

    resource_waiter_t(int resource_required, function<void (task_t *)> on_block = [](task_t *){}
             , function<void (void)> on_release = [](){})
        : required_(resource_required), on_block_(std::move(on_block))
        , on_release_(std::move(on_release)) {}

    bool wait(task_t *current_task) {
        utility_flag_helper<task_blocked, no_utility_flag> ufh(current_task);
        on_block_(current_task);
        return signal_.wait(max_response, [current_task](){
            return current_task->stop_flag() || current_task->is_yielding();
        });
    }

    void adjust_resource(int increment) {
        bool enough = false;
        signal_.notify_all([this, increment, &enough](){
            enough = required_ < (counter_ += increment);
            if(enough || counter_ < 0){
                counter_ = 0;
            }
            return enough;
        });
        if(enough){
            on_release_();
        }
    }

    void release_tasks() { adjust_resource(required_); }

    void reset() { signal_.reset(); counter_ = 0; }

};

}

using waiter_nms::resource_waiter_t;
