#pragma once

#include <vector>
#include <list>
#include <unordered_map>
#include <atomic>
#include <utility>
#include <shared_mutex>
#include <memory>
#include <string>
#include <cassert>
#include <cstdint>
#include <functional>
#include <cstring>
#include <thread>
#include <cstdlib>
#include <iterator>

#include "tasks/task.h"
#include "memory/buffer.h"

namespace conveyer_nms {

using std::vector;
using std::list;
using std::unordered_map;
using std::atomic_flag;
using std::atomic_int;
using std::pair;
using std::shared_mutex;
using std::lock_guard;
using std::shared_lock;
using std::shared_ptr;
using std::unique_ptr;
using std::string;
using std::function;
using namespace std::chrono_literals;

namespace transfer_flags {

const int operational_error = -3;
const int descriptor_error = -2;
const int descriptor_shutdown = -1;
const int no_transfer_flag = 0;
const int data_pending = 1;

}

using namespace transfer_flags;

enum transfer_indices { writer_index = 0, reader_index_start = 1 };

class transfer_line_t {
    string description_;
    buffer_t buffer_;
    const unsigned index_cnt_;
    vector<atomic_flag> locks_;
    vector<atomic_int> flags_;
    vector<task_t *> tasks_;

public:

    transfer_line_t(const string &description, unsigned lane_cnt
                    , memory_pager_t *pager)
        : description_(description), buffer_(lane_cnt, pager), index_cnt_(lane_cnt + 1)
        , locks_(index_cnt_), flags_(index_cnt_), tasks_(index_cnt_, nullptr) {
        for(auto &l : locks_){
            l.clear(std::memory_order_relaxed);
        }
        for(auto &f : flags_){
            f.store(no_transfer_flag, std::memory_order_relaxed);
        }
    }

    const string &description() const { return description_; }

    unsigned index_count() const { return index_cnt_; }

    int transfer_flag(unsigned idx, std::memory_order mo = std::memory_order_acquire) const {
        assert(idx < flags_.size());
        int val = flags_[idx].load(mo);
        return val;
    }

    void set_transfer_flag(unsigned idx, int val
                           , std::memory_order mo = std::memory_order_release) {
        assert(idx < flags_.size());
        flags_[idx].store(val, mo);
    }

    bool acquire_buffer_lock(task_t *task, unsigned idx, bool force = false) {
        assert(idx < locks_.size() && idx < tasks_.size());
        while(locks_[idx].test_and_set(std::memory_order_acquire)){
            if(!force){
                return false;
            }
            std::this_thread::sleep_for(1ms);
        }
        tasks_[idx] = task;
        return true;
    }

    void release_buffer_lock(unsigned idx) {
        assert(idx < locks_.size() && idx < tasks_.size());
        tasks_[idx] = nullptr;
        locks_[idx].clear(std::memory_order_release);
    }

    buffer_t *buffer() { return &buffer_; }

    task_t *active_task(unsigned idx) {
        assert(idx < tasks_.size());
        return tasks_[idx];
    }
};

using Descriptor = int;

const Descriptor invalid_descriptor = -1;

class transfer_handle_t {
    Descriptor descriptor_;
    transfer_line_t *line_;
    unsigned idx_;
    bool valid_;

    void release_buffer_lock() const { if(valid_){ line_->release_buffer_lock(idx_); } }

    void acquire_buffer_lock(task_t *task, bool force) {
        if(valid_){
            valid_ = line_->acquire_buffer_lock(task, idx_, force);
        }
    }

    void move(transfer_handle_t &&other) {
        descriptor_ = other.descriptor_;
        line_ = other.line_;
        idx_ = other.idx_;
        valid_ = other.valid_;
        other.valid_ = false;
    }

    transfer_handle_t(transfer_handle_t &) = delete;
    transfer_handle_t &operator=(transfer_handle_t &) = delete;

public:

    ~transfer_handle_t() { release_buffer_lock(); }

    Descriptor descriptor() const { return descriptor_; }

    bool is_valid() const { return valid_; }

    void set_transfer_flag(int val) const {
        assert(valid_);
        line_->set_transfer_flag(idx_, val);
    }

    int transfer_flag() const {
        assert(line_);
        return line_->transfer_flag(idx_, std::memory_order_relaxed);
    }

    const string &description() const { assert(line_); return line_->description(); }

    transfer_handle_t(transfer_handle_t &&other) { move(std::move(other)); }

    transfer_handle_t &operator=(transfer_handle_t &&other) {
        release_buffer_lock();
        move(std::move(other));
        return *this;
    }

protected:

    transfer_handle_t(task_t *task, Descriptor descriptor, transfer_line_t *line, unsigned index
                      , bool force_lock)
        : descriptor_(descriptor), line_(line), idx_(index), valid_(line_) {
        acquire_buffer_lock(task, force_lock);
    }

    buffer_t *buffer() const { assert(valid_); return line_->buffer(); }
};

class write_handle_t : public transfer_handle_t {
public:

    write_handle_t(task_t *task, Descriptor descriptor, transfer_line_t *line
                   , bool force_lock = false)
        : transfer_handle_t(task, descriptor, line, writer_index, force_lock) {}

    unsigned advance(unsigned bytes_written) const {
        return buffer()->advance_writer(bytes_written);
    }

    shared_ptr<page_t> page() const { return buffer()->writer_page(); }

    unsigned pos() const { return buffer()->writer_pos(); }
};

class read_handle_t : public transfer_handle_t {
    const unsigned lane_num_;

public:

    read_handle_t(task_t *task, Descriptor descriptor, transfer_line_t *line, unsigned lane_num
                  , bool force_lock = false)
        : transfer_handle_t(task, descriptor, line, reader_index_start + lane_num, force_lock)
        , lane_num_(lane_num) {}

    unsigned advance(unsigned bytes_read) const {
        return buffer()->advance_reader(lane_num_, bytes_read);
    }

    shared_ptr<page_t> page() const { return buffer()->reader_page(lane_num_); }

    unsigned pos() const { return buffer()->reader_pos(lane_num_); }
};

struct server_side {};

struct clients_side {};

class peer_t {
    Descriptor client_descriptor_;
    Descriptor server_descriptor_;

    peer_t(const peer_t &) = delete;
    peer_t &operator=(const peer_t &) = delete;
    peer_t(peer_t &&) = delete;
    peer_t &operator=(peer_t &&) = delete;

public:

    peer_t(Descriptor client_descriptor, Descriptor server_descriptor)
        : client_descriptor_(client_descriptor), server_descriptor_(server_descriptor) {}

    virtual ~peer_t() {}

    template <class ConveyerSide> Descriptor descriptor() const;
};

template<> inline Descriptor peer_t::descriptor<server_side>() const { return server_descriptor_; }

template<> inline Descriptor peer_t::descriptor<clients_side>() const { return client_descriptor_; }

class transfer_loop_t {
    transfer_line_t client_line_;
    transfer_line_t server_line_;
    unique_ptr<peer_t> peer_;

public:

    transfer_loop_t(const string &client_description, const string &server_description
                , unsigned lane_cnt, memory_pager_t *pager, unique_ptr<peer_t> &&peer)
        : client_line_(client_description, lane_cnt, pager)
        , server_line_(server_description, lane_cnt, pager)
        , peer_(std::move(peer)) { assert(peer_); }

    template <class ConveyerSide>
    Descriptor descriptor() const { return peer_->descriptor<ConveyerSide>(); }

    transfer_line_t *line(Descriptor d) {
        if(descriptor<server_side>() == d){
            return &server_line_;
        }
        if(descriptor<clients_side>() == d){
            return &client_line_;
        }
        return nullptr;
    }

    template <class ConveyerSide> transfer_line_t *line();
};

template <> inline
transfer_line_t *transfer_loop_t::line<server_side>() { return &server_line_; }

template <> inline
transfer_line_t *transfer_loop_t::line<clients_side>() { return &client_line_; }

constexpr auto always_true = [](int) { return true; };

const unsigned one_time_max = 65536;

class page_wrapper_t {
    shared_ptr<page_t> page_;
    unsigned pos_;
    unsigned sz_;

public:

    page_wrapper_t() = default;

    page_wrapper_t(shared_ptr<page_t> &&page, unsigned pos, unsigned sz)
        : page_(std::move(page)), pos_(pos), sz_(sz) {}

   int8_t *data() const { return page_->data() + pos_; }

   unsigned size() const { return sz_; }

   void adjust_pos(unsigned inc) { assert(inc <= sz_); pos_ += inc; sz_ -= inc; }
};

class transfer_conveyer_t {
    const unsigned lane_cnt_;
    memory_pager_t *pager_;
    list<transfer_loop_t> conveyer_;

public:

    using conveyer_iterator = decltype(conveyer_)::iterator;

private:

    unordered_map<Descriptor, conveyer_iterator> descriptors_hash_;
    shared_mutex conveyer_mutex_;

    conveyer_iterator find_loop(Descriptor descriptor) {
        auto it = descriptors_hash_.find(descriptor);
        if(it != descriptors_hash_.end()){
            return it->second;
        }
        return conveyer_.end();
    }

    transfer_line_t *line(Descriptor descriptor) {
        auto it_loop = find_loop(descriptor);
        if(it_loop != conveyer_.end()){
            return it_loop->line(descriptor);
        }
        return nullptr;
    }

    conveyer_iterator start_after_prev(Descriptor descriptor) {
        auto start = conveyer_.begin();
        if(descriptor != invalid_descriptor){
            auto prev = find_loop(descriptor);
            if(prev != conveyer_.end()){
                start = ++prev;
            }
        }
        return start;
    }

    template <class ConveyerSide, class PredicateF, class HandlerMakerF>
    auto find_handler(const PredicateF &pred, const HandlerMakerF &maker, Descriptor prev){
        shared_lock<shared_mutex> sl(conveyer_mutex_);
        for(auto it = start_after_prev(prev); it != conveyer_.end(); ++it){
            auto line = it->line<ConveyerSide>();
            if(pred(line)){
                return maker(it->descriptor<ConveyerSide>(), line);
            }
        }
        return maker(invalid_descriptor, nullptr);
    }

    template <class ConveyerSide, class FlagPredicate>
    read_handle_t find_to_read(task_t *task, const FlagPredicate &pred, unsigned lane_num
                               , Descriptor prev = invalid_descriptor) {
        return find_handler<ConveyerSide>([&pred, lane_num](transfer_line_t *line){
            return pred(line->transfer_flag(reader_index_start + lane_num)); }
        , [task, lane_num](Descriptor descriptor, transfer_line_t *line){
            return read_handle_t(task, descriptor, line, lane_num); }
        , prev);
    }

    template <class ConveyerSide, class FlagPredicate>
    write_handle_t find_to_write(task_t *task, const FlagPredicate &pred
                                 , Descriptor prev = invalid_descriptor) {
        return find_handler<ConveyerSide>([&pred](transfer_line_t *line){
            return pred(line->transfer_flag(writer_index)); }
        , [task](Descriptor descriptor, transfer_line_t *line){
            return write_handle_t(task, descriptor, line); }
        , prev);
    }

    template <class FlagPredicate, class O, class FindHandleF>
    unsigned iterate(const FlagPredicate &pred, const O &operation, const FindHandleF &find_f) {
        auto prev = invalid_descriptor;
        unsigned peers_processed = 0;
        for(;;){
            auto handle = find_f(pred, prev);
            prev = handle.descriptor();
            if(prev == invalid_descriptor){
                break;
            }
            if(handle.is_valid()){
                if(pred(handle.transfer_flag())){
                    if(operation(handle)){
                        ++peers_processed;
                    }
                }
            }
        }
        return peers_processed;
    }

    template <class MessageExceptF, class ExceptF, class GetDataF>
    bool write_operation(const MessageExceptF &message_f, const ExceptF &except_f
                         , const GetDataF &get_f, const write_handle_t &handle) {
        int flag = handle.transfer_flag();
        unsigned total_written = 0;
        while(total_written < one_time_max){
            const int8_t *data;
            unsigned to_write = get_f(handle.description(), handle.descriptor(), &data, &flag);
            if(to_write == 0){
                break;
            }
            total_written += to_write;
            unsigned bytes_written = 0;
            for(;;){
                unsigned bytes_available;
                if(except_f([&](){ bytes_available = handle.advance(bytes_written); })){
                    if(to_write == 0){
                        break;
                    }
                    std::shared_ptr<page_t> page;
                    if(except_f([&](){ page = handle.page(); })){
                        bytes_written = to_write < bytes_available ? to_write : bytes_available;
                        memcpy(page->data() + handle.pos(), data, bytes_written);
                        data += bytes_written;
                        to_write -= bytes_written;
                        continue;
                    }
                }
                message_f("unable to write data", [&](){
                    return handle.description() + " : unable to write data";
                });
                handle.set_transfer_flag(operational_error);
                return false;
            }
        }
        handle.set_transfer_flag(flag);
        return total_written != 0;
    }

    template <class MessageExceptF, class ExceptF, class TakeDataF>
    bool read_operation(const MessageExceptF &message_f, const ExceptF &except_f
                        , const TakeDataF &take_f, const read_handle_t &handle) {
        int orig_flag = handle.transfer_flag();
        int flag = orig_flag;
        unsigned total_read = 0;
        unsigned bytes_read = 0;
        for(;;){
            unsigned to_read;
            if(except_f([&](){ to_read = handle.advance(bytes_read); })){
                if(to_read == 0 || one_time_max < total_read){
                    break;
                }
                shared_ptr<page_t> page;
                if(except_f([&](){ page = handle.page(); })){
                    flag = orig_flag;
                    page_wrapper_t wpage;
                    bool ok = except_f([&](){
                        wpage = page_wrapper_t(std::move(page), handle.pos(), to_read);
                    });
                    if(ok){
                        bytes_read = take_f(handle.description(), handle.descriptor()
                                            , std::move(wpage), &flag);
                        if(bytes_read <= 0){
                            break;
                        }
                        total_read += bytes_read;
                        continue;
                    }
                }
            }
            message_f("unable to read data", [&](){
                return handle.description() + " : unable to read data";
            });
            handle.set_transfer_flag(operational_error);
            return false;
        }
        handle.set_transfer_flag(flag);
        return total_read != 0;
    }

    template <class MessageExceptF, class ExceptF>
    bool ready_read_operation(const MessageExceptF &message_f, const ExceptF &except_f
                              , const read_handle_t &handle) {
        unsigned to_read;
        if(except_f([&](){ to_read = handle.advance(0); })){
            return 0 < to_read;
        }
        message_f("unable to check data availability", [&](){
            return handle.description() + " : unable to check data availability";
        });
        handle.set_transfer_flag(operational_error);
        return false;
    }

    read_handle_t read_handle(task_t *task, Descriptor descriptor, unsigned lane_num) {
        shared_lock<shared_mutex> sl(conveyer_mutex_);
        return read_handle_t(task, descriptor, line(descriptor), lane_num, true);
    }

    write_handle_t write_handle(task_t *task, Descriptor descriptor) {
        shared_lock<shared_mutex> sl(conveyer_mutex_);
        return write_handle_t(task, descriptor, line(descriptor), true);
    }

    template <class FlagF, class HandleT>
    bool flag_handle(const HandleT &handle, const FlagF &flag_f) {
        if(handle.is_valid()){
            int flag = handle.transfer_flag();
            if(flag_f(&flag)){
                handle.set_transfer_flag(flag);
                return true;
            }
        }
        return false;
    }

    template <class MessageExceptF, class ClearF>
    conveyer_iterator drop_peer(conveyer_iterator it, const MessageExceptF &message_f
                                , const ClearF &clear_f) {
        const auto get_locks = [&message_f](transfer_line_t *line){
            for(unsigned idx = 0; idx < line->index_count(); ++idx){
                bool done;
                task_t *task = nullptr;
                auto delay = 0ms;
                const auto desperate_delay = max_response * 10;
                while(!(done = line->acquire_buffer_lock(nullptr, idx))
                      && delay < desperate_delay){
                    task = line->active_task(idx);
                    auto sleep_delay = 1ms;
                    if(task){
                        if(task->utility_flag() == task_blocked){
                            task->yield();
                            sleep_delay += max_response;
                        }
                    }
                    std::this_thread::sleep_for(sleep_delay);
                    delay += sleep_delay;
                }
                if(!done){
                    message_f("unable to remove peer", [line, task, idx](){
                        string msg = "unable to remove peer : [" + line->description() + ']'
                                + " lane " + std::to_string(idx) + " is blocked";
                        if(task){
                            msg += " by task \"";
                            msg += task->name();
                            msg += '\"';
                        } else{
                            msg += ", task pointer is null";
                        }
                        return msg;
                    });
                    return false;
                }
            }
            return true;
        };
        if(!get_locks(it->line<clients_side>())){
            return it;
        }
        if(!get_locks(it->line<server_side>())){
            return it;
        }
        auto client_descriptor = it->descriptor<clients_side>();
        auto server_descriptor = it->descriptor<server_side>();
        descriptors_hash_.erase(client_descriptor);
        descriptors_hash_.erase(server_descriptor);
        clear_f(client_descriptor, server_descriptor);
        return conveyer_.erase(it);
    }

public:

    transfer_conveyer_t(unsigned lane_cnt, memory_pager_t *pager)
        : lane_cnt_(lane_cnt), pager_(pager) { assert(lane_cnt_); }

    unsigned lane_count() const { return lane_cnt_; }

    conveyer_iterator add_peer(const string &peer_name, unique_ptr<peer_t> &&peer) {
        decltype(descriptors_hash_)::iterator cit, sit;
        bool client_inserted = false;
        bool server_inserted = false;
        lock_guard<shared_mutex> lg(conveyer_mutex_);
        try{
            auto end = conveyer_.end();
            auto res = descriptors_hash_.insert({ peer->descriptor<clients_side>(), end });
            assert(res.second);
            client_inserted = res.second;
            cit = res.first;
            res = descriptors_hash_.insert({ peer->descriptor<server_side>(), end });
            assert(res.second);
            server_inserted = res.second;
            sit = res.first;
            auto it = conveyer_.emplace(end, "from " + peer_name, "to " + peer_name
                                        , lane_cnt_, pager_, std::move(peer));
            cit->second = it;
            sit->second = it;
            return it;
        } catch(...){
            if(client_inserted){
                descriptors_hash_.erase(cit);
            }
            if(server_inserted){
                descriptors_hash_.erase(sit);
            }
            throw;
        }
    }

    template <class FlagPredicate, class MessageExceptF, class ClearF>
    void drop_peers(const FlagPredicate &pred, const MessageExceptF &message_f
                    , const ClearF &clear_f) {
        const auto check_flags = [&pred](transfer_line_t *line){
            for(unsigned idx = 0; idx < line->index_count(); ++idx){
                if(pred(line->transfer_flag(idx))){
                    return true;
                }
            }
            return false;
        };
        shared_lock<shared_mutex> sl(conveyer_mutex_);
        for(auto it = conveyer_.begin(); it != conveyer_.end();){
            if(check_flags(it->line<clients_side>()) || check_flags(it->line<server_side>())){
                sl.unlock();
                {
                    lock_guard<shared_mutex> lg(conveyer_mutex_);
                    it = drop_peer(it, message_f, clear_f);
                }
                sl.lock();
            } else{
                ++it;
            }
        }
    }

    template <class MessageExceptF, class ClearF>
    void drop_random_peer(const MessageExceptF &message_f, const ClearF &clear_f) {
        if(!conveyer_.size()){
            return;
        }
        lock_guard<shared_mutex> lg(conveyer_mutex_);
        auto it = conveyer_.begin();
        std::advance(it, std::rand() * (conveyer_.size() - 1) / RAND_MAX);
        drop_peer(it, message_f, clear_f);
    }

    void clear() {
        lock_guard<shared_mutex> lg(conveyer_mutex_);
        descriptors_hash_.clear();
        conveyer_.clear();
    }

    unsigned peers_count() {
        shared_lock<shared_mutex> sl(conveyer_mutex_);
        return conveyer_.size();
    }

    template <class ConveyerSide, class MessageExceptF, class ExceptF, class GetDataF
              , class FlagPredicate = decltype(always_true)>
    unsigned write(task_t *task, const MessageExceptF &message_f, const ExceptF &except_f
                   , const GetDataF &get_f, const FlagPredicate &pred = always_true) {
        return iterate(pred, [&](const write_handle_t &handle){
            return write_operation(message_f, except_f, get_f, handle);
        }, [this, task](const FlagPredicate &pred, Descriptor descriptor) {
            return find_to_write<ConveyerSide>(task, pred, descriptor);
        });
    }

    template <class MessageExceptF, class ExceptF, class GetDataF>
    bool write(task_t *task, Descriptor descriptor, const MessageExceptF &message_f, const ExceptF &except_f
               , const GetDataF &get_f) {
        write_handle_t handle = write_handle(task, descriptor);
        if(handle.is_valid()){
            return write_operation(message_f, except_f, get_f, handle);
        }
        return false;
    }

    template <class ConveyerSide, class MessageExceptF, class ExceptF, class TakeDataF
              , class FlagPredicate = decltype(always_true)>
    unsigned read(task_t *task, unsigned lane_num, const MessageExceptF &message_f
                  , const ExceptF &except_f, const TakeDataF &take_f
                  , const FlagPredicate &pred = always_true) {
        return iterate(pred, [&](const read_handle_t &handle){
            return read_operation(message_f, except_f, take_f, handle);
        }, [this, task, lane_num](const FlagPredicate &pred, Descriptor descriptor) {
            return find_to_read<ConveyerSide>(task, pred, lane_num, descriptor);
        });
    }

    template <class MessageExceptF, class ExceptF, class TakeDataF>
    bool read(task_t *task, Descriptor descriptor, unsigned lane_num, const MessageExceptF &message_f
              , const ExceptF &except_f, const TakeDataF &take_f) {
        read_handle_t handle = read_handle(task, descriptor, lane_num);
        if(handle.is_valid()){
            return read_operation(message_f, except_f, take_f, handle);
        }
        return false;
    }

    template <class ConveyerSide, class MessageExceptF, class ExceptF
              , class FlagPredicate = decltype(always_true)>
    unsigned ready_read(task_t *task, unsigned lane_num, const MessageExceptF &message_f
                        , const ExceptF &except_f, const FlagPredicate &pred = always_true) {
        return iterate(pred, [&](const read_handle_t &handle){
            return ready_read_operation(message_f, except_f, handle);
        }, [this, task, lane_num](const FlagPredicate &pred, Descriptor descriptor) {
            return find_to_read<ConveyerSide>(task, pred, lane_num, descriptor);
        });
    }

    template <class FlagF> bool flag(task_t *task, Descriptor descriptor, unsigned lane_num
                                     , const FlagF &flag_f) {
        read_handle_t handle = read_handle(task, descriptor, lane_num);
        return flag_handle(handle, flag_f);
    }

    template <class FlagF> bool flag(task_t *task, Descriptor descriptor, const FlagF &flag_f) {
        write_handle_t handle = write_handle(task, descriptor);
        return flag_handle(handle, flag_f);
    }

    Descriptor other_side(Descriptor descriptor) {
        shared_lock<shared_mutex> sl(conveyer_mutex_);
        auto loop = find_loop(descriptor);
        if(loop != conveyer_.end()){
            if(loop->descriptor<clients_side>() == descriptor){
                return loop->descriptor<server_side>();
            }
            if(loop->descriptor<server_side>() == descriptor){
                return loop->descriptor<clients_side>();
            }
        }
        return invalid_descriptor;
    }
};

}

using conveyer_nms::transfer_conveyer_t;
using conveyer_nms::peer_t;
using conveyer_nms::server_side;
using conveyer_nms::clients_side;
using conveyer_nms::operational_error;
using conveyer_nms::descriptor_error;
using conveyer_nms::descriptor_shutdown;
using conveyer_nms::no_transfer_flag;
using conveyer_nms::data_pending;
using conveyer_nms::page_wrapper_t;
