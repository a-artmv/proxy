#pragma once

#include <functional>
#include <utility>

namespace destructoid_nms {

using std::function;

class destructoid_t {
    typedef function<void (void)> action_t;
    action_t action_;

    destructoid_t(const destructoid_t &) = delete;
    destructoid_t &operator=(const destructoid_t &) = delete;

    void move_from(destructoid_t &other){
        action_ = std::move(other.action_);
        other.action_ = action_t();
    }

    void act() const { if(action_){ action_(); } }

    action_t get() const { return action_ ? action_ : [](){}; }

public:

    destructoid_t() = default;

    destructoid_t(destructoid_t &&other) { move_from(other); }

    destructoid_t &operator=(destructoid_t &&other) { act(); move_from(other); return *this; }

    ~destructoid_t() { act(); }

    void swap(destructoid_t &other) { action_.swap(other.action_); }

    template <class O> void append(const O &op) {
        action_t a([prev = get(), o = op](){ prev(); o(); });
        action_.swap(a);
    }

    template <class O> void prepend(const O &op) {
        action_t a([prev = get(), o = op](){ o(); prev(); });
        action_.swap(a);
    }
};

}

using destructoid_nms::destructoid_t;
