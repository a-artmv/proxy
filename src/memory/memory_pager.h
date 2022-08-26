#pragma once

#include <new>
#include <cstdint>
#include <memory>
#include <utility>
#include <atomic>

#include "../synchronization/resource_waiter.h"
#include "cache.h"

namespace pager_nms {

using std::shared_ptr;
using std::unique_ptr;
using std::atomic_uint;

class memory_pager_t {
    const bool prefill_cache_;
    cache_t<int8_t[]> memory_cache_;
    resource_waiter_t *memory_waiter_;
    const unsigned page_size_;
    atomic_uint release_counter_ = { 0 };

public:

    class page_t {
        memory_pager_t *parent_;
        unique_ptr<int8_t[]> memory_;

        page_t(const page_t &) = delete;
        page_t operator=(const page_t &) = delete;

    public:

        page_t(memory_pager_t *parent, unique_ptr<int8_t[]> &&memory)
            : parent_(parent), memory_(std::move(memory)) {}

        ~page_t() { try { parent_->free(memory_); }catch(...) {} }

        int8_t *data() const { return memory_.get(); }

        unsigned size() const { return parent_->page_size(); }
    };

    memory_pager_t(resource_waiter_t *memory_waiter, unsigned page_size
            , unsigned cache_size, bool prefill_cache = true)
        : prefill_cache_(prefill_cache)
        , memory_cache_([page_size](){ return std::make_unique<int8_t[]>(page_size); }
                        , cache_size, prefill_cache_)
        , memory_waiter_(memory_waiter), page_size_(page_size) {}

    shared_ptr<page_t> get_page() {
        auto p = std::make_shared<page_t>(this, memory_cache_.take());
        memory_waiter_->adjust_resource(-1);
        return p;
    }

    void reset() {
        release_counter_.store(0, std::memory_order_release);
        memory_cache_.reset(prefill_cache_);
    }

    unsigned page_size() const { return page_size_; }

    unsigned cache_size() const { return memory_cache_.size(); }

    unsigned pages_available() const { return memory_cache_.elements_available(); }

    resource_waiter_t *memory_waiter() const { return memory_waiter_; }

    unsigned release_counter() const { return release_counter_.load(std::memory_order_acquire); }

private:

    void free(unique_ptr<int8_t[]> &memory) {
        memory_cache_.store(memory);
        memory_waiter_->adjust_resource(1);
        release_counter_.fetch_add(1, std::memory_order_release);
    }
};

}

using pager_nms::memory_pager_t;
using page_t = pager_nms::memory_pager_t::page_t;
