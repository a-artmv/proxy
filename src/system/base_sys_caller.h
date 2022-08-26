#pragma once

#include <system_error>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace base_sys_caller_nms {

using std::function;
using std::string;
using std::system_error;
using std::vector;

typedef const function<void ()> &operation_t;

constexpr auto no_catch_f = [](operation_t op){ op(); return true; };

class base_sys_caller_t {
    function<void (const char *)> message_;
    function<int ()> get_error_;
    function<bool (operation_t)> except_f_;
    function<bool (int)> ignore_;
    function<bool (int)> retry_on_;

    template <class A> void act_or_ignore(int error, A action) {
        if(!ignore_(error)){
            action();
        }
    }

    template <class A, class F> int call_and_act(A action, const F &bound_f) {
        for(;;){
            int res = bound_f();
            if(res == -1){
                auto error = get_error_();
                if(retry_on_(error)){
                    continue;
                }
                act_or_ignore(error, action);
            }
            return res;
        }
    }

    template <class A, class F> int call_and_act_once(A action, const F &bound_f) {
        int res = bound_f();
        if(res == -1){
            auto error = get_error_();
            if(!retry_on_(error)){
                act_or_ignore(error, action);
            }
        }
        return res;
    }

    void message_action(const char *preamble) { message_error(preamble, get_error_()); }

    void throw_action(const char *preamble) { throw_error(preamble, get_error_()); }

public:

    static constexpr struct call_once_t {} call_once = {};
    static constexpr struct call_retry_t {} call_retry = {};
    static constexpr struct throw_on_error_t {} throw_on_error = {};
    static constexpr struct message_on_error_t {} message_on_error = {};

    base_sys_caller_t(const function<void (const char *)> &message_f
                      , const function<int ()> &get_error_f
                      , vector<int> errors_to_ignore, vector<int> errors_to_retry_on
                      , const function<bool (operation_t)> &except_f = no_catch_f)
        : message_(message_f), get_error_(get_error_f), except_f_(except_f) {
        const auto check_list = [](const vector<int> &errors, int error){
            for(const auto &e : errors){
                if(e == error){
                    return true;
                }
            }
            return false;
        };
        ignore_ = [ignore_list = std::move(errors_to_ignore), check_list](int error){
            return check_list(ignore_list, error);
        };
        retry_on_ = [retry_list = std::move(errors_to_retry_on), check_list](int error){
            return check_list(retry_list, error);
        };
    }

    void show_message(const char *message) { message_(message); }

    void message_error(const char *preamble, int error) {
        string msg;
        if(except_f_([&msg, preamble, error](){
                      msg = preamble;
                      msg += ": ";
                      msg += std::generic_category().default_error_condition(error).message(); })){
            message_(msg.c_str());
        }
    }

    void throw_error(const char *preamble, int error) {
        throw system_error(error, std::generic_category(), preamble);
    }

    template <class F>
    int call(throw_on_error_t, const char *preamble, const F &bound_f, call_retry_t) {
        return call_and_act([this, preamble](){ throw_action(preamble); }, bound_f);
    }

    template <class F>
    int call(message_on_error_t, const char *preamble, const F &bound_f, call_retry_t) {
        return call_and_act([this, preamble](){ message_action(preamble); }, bound_f);
    }

    template <class F>
    int call(throw_on_error_t, const char *preamble, const F &bound_f, call_once_t) {
        return call_and_act_once([this, preamble](){ throw_action(preamble); }, bound_f);
    }

    template <class F>
    int call(message_on_error_t, const char *preamble, const F &bound_f, call_once_t) {
        return call_and_act_once([this, preamble](){ message_action(preamble); }, bound_f);
    }

    template <class F, class OnError>
    int call(OnError a, const char *preamble, const F &bound_f) {
        return call(a, preamble, bound_f, call_retry);
    }
};

}

using base_sys_caller_nms::base_sys_caller_t;
using base_sys_caller_nms::no_catch_f;
using base_sys_caller_nms::operation_t;
