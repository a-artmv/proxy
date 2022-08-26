#pragma once

#include <functional>
#include <new>
#include <string>

#include "destructoid.h"
#include "../tasks/task.h"
#include "../synchronization/resource_waiter.h"

namespace exceptor_nms {

using std::function;
using std::string;

class exceptor_t : public task_t {
    resource_waiter_t *memory_waiter_;
    function<void (const char *)> message_;

    template <class O>
    bool destructoid_f(destructoid_t *cleaner, const O &op, void (destructoid_t::*f)(const O &)) {
        if(!except([&](){ (cleaner->*f)(op); })){
            op();
            return false;
        }
        return true;
    }

public:

    exceptor_t(resource_waiter_t *memory_waiter, task_control_t *ctrl
               , const function<void (const char *)> &message_f)
        : task_t(ctrl), memory_waiter_(memory_waiter), message_(message_f) {}

protected:

    resource_waiter_t *memory_waiter() const { return memory_waiter_; }

    template <class O> bool except(const O &op) {
        for(;;){
            try{
                op();
            } catch(std::bad_alloc &){
                if(!memory_waiter_->wait(this)){
                    if(!stop_flag()){
                        message_("waiting for memory cancelled");
                    }
                    return false;
                }
                continue;
            } catch(std::exception &e){
                message_(e.what());
                return false;
            } catch(...){
                message_("unknown exception");
                return false;
            }
            break;
        }
        return true;
    }

    template <class FullMessageF>
    void show_message_except(const char *default_msg, const FullMessageF &full_msg_f) {
        const char *msg = default_msg;
        string str;
        if(except([&](){ str = full_msg_f(); })){
            msg = str.c_str();
        }
        message_(msg);
    }

    void show_message_simple(const char *msg) const { message_(msg); }

    template <class O> bool append_or_call(destructoid_t *cleaner, const O &op) {
        return destructoid_f(cleaner, op, &destructoid_t::append);
    }

    template <class O> bool prepend_or_call(destructoid_t *cleaner, const O &op) {
        return destructoid_f(cleaner, op, &destructoid_t::prepend);
    }
};

}

using exceptor_nms::exceptor_t;
