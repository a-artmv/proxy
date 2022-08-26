#pragma once

#include <memory>
#include <utility>
#include <mutex>
#include <queue>
#include <vector>
#include <cassert>

#include "memory_pager.h"

namespace buffer_nms {

using std::shared_ptr;
using std::mutex;
using std::lock_guard;
using std::queue;
using std::vector;

class buffer_t {

    class lane_t {

        struct node_t {
            shared_ptr<page_t> page;
            unsigned pos;
            unsigned data_size;
        };

        queue<node_t> queue_;
        mutex mx_;
        shared_ptr<page_t> last_page_{ nullptr };
        unsigned last_pos_;

    public:

        lane_t() {}

        shared_ptr<page_t> page() {
            assert(!queue_.empty());
            lock_guard<mutex> lg(mx_);
            return queue_.front().page;
        }

        unsigned advance(unsigned bytes_read) {
            lock_guard<mutex> lg(mx_);
            while(!queue_.empty()){
                auto &front = queue_.front();
                front.pos += bytes_read;
                assert(front.pos <= front.data_size);
                if(front.pos < front.data_size){
                    return front.data_size - front.pos;
                }
                last_page_ = queue_.front().page;
                last_pos_ = queue_.front().pos;
                queue_.pop();
                bytes_read = 0;
            }
            return 0;
        }

        void put(shared_ptr<page_t> writer_page, unsigned data_size) {
            lock_guard<mutex> lg(mx_);
            if(!queue_.empty()){
                auto &back = queue_.back();
                if(back.page->data() == writer_page->data()){
                    back.data_size = data_size;
                    return;
                }
            }
            if(last_page_){
                if(last_page_->data() == writer_page->data()){
                    queue_.push({writer_page, last_pos_, data_size});
                    last_page_ = nullptr;
                    return;
                }
            }
            queue_.push({writer_page, 0, data_size});
        }

        unsigned pos() const {
            unsigned res = 0;
            if(!queue_.empty()){
                res = queue_.front().pos;
            }
            return res;
        }
    };

    vector<lane_t> readers_lanes_;
    memory_pager_t *pager_;
    unsigned writer_pos_;
    shared_ptr<page_t> writer_page_;

public:

    buffer_t(unsigned lane_cnt, memory_pager_t *pager)
        : readers_lanes_(lane_cnt), pager_(pager), writer_pos_(pager->page_size()) {}

    unsigned advance_writer(unsigned bytes_written) {
        unsigned pos = writer_pos_ + bytes_written;
        unsigned page_size = pager_->page_size();
        assert(pos <= page_size);
        if(bytes_written){
            for(auto &lane : readers_lanes_){
                lane.put(writer_page_, pos);
            }
        }
        writer_pos_ = pos;
        if(writer_pos_ < page_size){
            return page_size - writer_pos_;
        }
        writer_page_ = nullptr;
        writer_pos_ = 0;
        return page_size;
    }

    shared_ptr<page_t> writer_page() {
        if(!writer_page_){
            writer_page_ = pager_->get_page();
        }
        return writer_page_;
    }

    unsigned writer_pos() const { return writer_pos_; }

    unsigned advance_reader(unsigned lane_num, unsigned bytes_read) {
        assert(lane_num < readers_lanes_.size());
        return readers_lanes_[lane_num].advance(bytes_read);
    }

    shared_ptr<page_t> reader_page(unsigned lane_num) {
        assert(lane_num < readers_lanes_.size());
        return readers_lanes_[lane_num].page();
    }

    unsigned reader_pos(unsigned lane_num) const {
        assert(lane_num < readers_lanes_.size());
        return readers_lanes_[lane_num].pos();
    }
};

}

using buffer_nms::buffer_t;
