#pragma once

#include <mutex>
#include <atomic>
#include <functional>
#include <algorithm>
#include <memory>
#include <utility>

namespace cache_nms {

using std::mutex;
using std::lock_guard;
using std::atomic_uint;
using std::function;
using std::unique_ptr;

template <class Element> class cache_t {
    unique_ptr<unique_ptr<Element>[]> cache_;
    function<unique_ptr<Element> (void)> create_;
    const unsigned size_;
    atomic_uint cur_size_ = { 0 };
    mutex mx_;

    void fill() {
        std::generate_n(&cache_[0], size_, create_);
        cur_size_.store(size_, std::memory_order_release);
    }

public:

    cache_t(function<unique_ptr<Element> (void)> create, unsigned size, bool prefill)
        : cache_(std::make_unique<unique_ptr<Element>[]>(size)), create_(std::move(create))
        , size_(size) { if(prefill){ fill(); } }

    unique_ptr<Element> take() {
        if(cur_size_.load(std::memory_order_acquire)){
            lock_guard<mutex> lg(mx_);
            if(cur_size_.load(std::memory_order_relaxed)){
                unique_ptr<Element> elem;
                std::swap(cache_[--cur_size_], elem);
                return elem;
            }
        }
        return create_();
    }

    void store(unique_ptr<Element> &elem) {
        if(cur_size_.load(std::memory_order_acquire) != size_){
            lock_guard<mutex> lg(mx_);
            if(cur_size_.load(std::memory_order_relaxed) != size_){
                std::swap(cache_[cur_size_++], elem);
                return;
            }
        }
    }

    void reset(bool prefill) {
        std::generate_n(&cache_[0], size_, [](){ return nullptr; });
        if(prefill){
            fill();
        } else{
            cur_size_.store(0, std::memory_order_release);
        }
    }

    unsigned elements_available() const { return cur_size_.load(std::memory_order_acquire); }

    unsigned size() const { return size_; }
};

}

using cache_nms::cache_t;
