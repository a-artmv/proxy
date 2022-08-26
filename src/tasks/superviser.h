#pragma once

#include "../exceptions/exceptor.h"
#include "../transfer_conveyer.h"
#include "../memory/memory_pager.h"
#include "psql_logger.h"

#include <vector>
#include <thread>
#include <mutex>

namespace superviser_nms {

using std::vector;
using std::mutex;

class superviser_t : public exceptor_t {
    transfer_conveyer_t *conveyer_;
    vector<task_control_t *> consumers_ctrls_;
    vector<task_control_t *> producers_ctrls_;
    vector<task_t *> consumers_;
    vector<task_t *> producers_;
    memory_pager_t *pager_;
    mutex mx_;

public:

    superviser_t(vector<task_control_t *> consumers_ctrls
                 , vector<task_control_t *> producers_ctrls
                 , vector<task_t *> consumers, vector<task_t *> producers
                 , task_control_t *ctrl, resource_waiter_t *memory_waiter
                 , transfer_conveyer_t *conveyer, memory_pager_t *pager
                 , const std::function<void (const char *)> &message_f)
        : exceptor_t(memory_waiter, ctrl, message_f), conveyer_(conveyer)
        , consumers_ctrls_(std::move(consumers_ctrls)), producers_ctrls_(std::move(producers_ctrls))
        , consumers_(std::move(consumers)), producers_(std::move(producers)), pager_(pager)
    {}

    const char *name() const override { return "superviser"; }

    void on_task_blocked(task_t *) {
        for(auto ctrl : consumers_ctrls_){
            ctrl->pause();
        }
    }

protected:

    bool one_step() override {
        const auto message_f = [this](const char *default_msg
                                      , const std::function<std::string ()> &full_msg){
            show_message_except(default_msg, full_msg);
        };
        const auto flag_pred = [](int transfer_flag){
            return transfer_flag == descriptor_shutdown || transfer_flag == descriptor_error
                    || transfer_flag == operational_error;
        };
        const auto clear_f = [](int cln_desc, int){
            psql_logger_t<clients_side>::clear_from(cln_desc);
        };
        conveyer_->drop_peers(flag_pred, message_f, clear_f);
        bool producers_blocked = false;
        for(auto task : producers_){
            if((producers_blocked = task->utility_flag() == task_blocked)){
                break;
            }
        }
        if(producers_blocked){
            pager_->memory_waiter()->release_tasks();
            if(pager_->pages_available() == 0){
                //conveyer_->drop_random_peer(message_f, clear_f);
            }
        } else{
            bool consumers_paused = false;
            for(auto ctrl : consumers_ctrls_){
                consumers_paused = ctrl->pause_flag();
                if(consumers_paused){
                    break;
                }
            }
            if(consumers_paused && pager_->cache_size() / 15 < pager_->pages_available()){
                for(auto ctrl : consumers_ctrls_){
                    ctrl->resume();
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return true;
    }

    bool on_start() override {
        show_message_simple("superviser thread started");
        return true;
    }

    void on_finish() override {
        show_message_simple("superviser thread finished");
    }
};

}

using superviser_nms::superviser_t;
