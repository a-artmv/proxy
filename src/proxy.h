#pragma once

#include <netinet/in.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <memory>
#include <algorithm>

#include "transfer_conveyer.h"
#include "tasks/task.h"
#include "tasks/connector.h"
#include "tasks/receiver.h"
#include "tasks/sender.h"
#include "tasks/logger.h"
#include "tasks/psql_logger.h"
#include "tasks/superviser.h"
#include "memory/memory_pager.h"
#include "synchronization/resource_waiter.h"
#include "synchronization/signal.h"
#include "synchronization/signal_pack.h"
#include "system/sys_caller.h"

namespace proxy_nms {

using std::vector;
using std::thread;
using std::unique_ptr;

const unsigned page_size = 4096;    // bytes
const unsigned cache_size = 8192;   // pages (32 MB)

const unsigned lanes_cnt = 2;       // 1 for sending + 1 for logging

class proxy_t : protected sys_caller_t {
    sockaddr_in proxy_addr_;
    signal_t *error_signal_;
    vector<thread> threads_;

    task_control_t superviser_ctrl_;
    task_control_t connectors_ctrl_;
    task_control_t clients_receivers_ctrl_;
    task_control_t server_receivers_ctrl_;
    task_control_t clients_senders_ctrl_;
    task_control_t server_senders_ctrl_;
    task_control_t clients_loggers_ctrl_;
    task_control_t server_loggers_ctrl_;

    resource_waiter_t memory_waiter_;
    memory_pager_t pager_;
    transfer_conveyer_t conveyer_;
    signal_pack_t clients_data_signal_;
    signal_pack_t server_data_signal_;

    const unsigned threading_level_;
    vector<unique_ptr<task_t>> connectors_;
    vector<unique_ptr<task_t>> clients_receivers_;
    vector<unique_ptr<task_t>> server_receivers_;
    vector<unique_ptr<task_t>> clients_senders_;
    vector<unique_ptr<task_t>> server_senders_;
    vector<unique_ptr<task_t>> clients_loggers_;
    vector<unique_ptr<task_t>> server_loggers_;
    unique_ptr<task_t> clients_senders_hlp_;
    unique_ptr<task_t> server_senders_hlp_;
    unique_ptr<superviser_t> superviser_;

    int connectors_epoll_;
    int clients_receivers_epoll_;
    int server_receivers_epoll_;
    int clients_senders_epoll_;
    int server_senders_epoll_;
    int listening_socket_;

    template<class O> void for_all_controls(O operation) {
        for(auto ctrl : { &superviser_ctrl_, &connectors_ctrl_
            , &clients_receivers_ctrl_, &server_receivers_ctrl_
            , &clients_senders_ctrl_, &server_senders_ctrl_
            , &clients_loggers_ctrl_, &server_loggers_ctrl_ }){
            operation(*ctrl);
        }
    }

    template<class O> void for_all_epolls(O operation) {
        for(auto fd : { &connectors_epoll_, &clients_receivers_epoll_, &server_receivers_epoll_
            , &clients_senders_epoll_, &server_senders_epoll_ }){
            operation(*fd);
        }
    }

    void task_blocked(task_t *t) { superviser_->on_task_blocked(t); }

public:

    proxy_t(signal_t *error_signal, const std::function<void (const char *)> &message_f
            , uint32_t proxy_address, uint16_t proxy_port, uint32_t srv_address, uint16_t srv_port)
        : sys_caller_t(message_f), error_signal_(error_signal)
        , memory_waiter_(cache_size / 5, [this](task_t *t){ task_blocked(t); }, [](){})
        , pager_(&memory_waiter_, page_size, cache_size), conveyer_(lanes_cnt, &pager_)
        , clients_data_signal_(lanes_cnt), server_data_signal_(lanes_cnt)
        , threading_level_(thread::hardware_concurrency()), connectors_(threading_level_)
        , clients_receivers_(threading_level_), server_receivers_(threading_level_)
        , clients_senders_(threading_level_), server_senders_(threading_level_)
        , clients_loggers_(threading_level_), server_loggers_(threading_level_) {
        clients_data_signal_.set_threading_level(threading_level_);
        server_data_signal_.set_threading_level(threading_level_);
        sockaddr_in server_addr;
        memset(&proxy_addr_, 0, sizeof (proxy_addr_));
        memset(&server_addr, 0, sizeof (proxy_addr_));
        proxy_addr_.sin_family = AF_INET;
        server_addr.sin_family = AF_INET;
        proxy_addr_.sin_addr.s_addr = proxy_address;
        server_addr.sin_addr.s_addr = srv_address;
        proxy_addr_.sin_port = proxy_port;
        server_addr.sin_port = srv_port;
        for_all_epolls([this](int &fd){
            fd = epoll_create(throw_on_error);
        });
        for(auto &uptr : connectors_){
            uptr = std::make_unique<connector_t>(server_addr, connectors_epoll_
                        , clients_receivers_epoll_, server_receivers_epoll_
                        , clients_senders_epoll_, server_senders_epoll_
                        , &connectors_ctrl_, &memory_waiter_, &conveyer_, message_f);
        }
        for(auto &uptr : clients_receivers_){
            uptr = std::make_unique<receiver_t<clients_side>>(&clients_data_signal_
                        , clients_receivers_epoll_, &clients_receivers_ctrl_
                        , &memory_waiter_, &conveyer_, message_f);
        }
        for(auto &uptr : server_receivers_){
            uptr = std::make_unique<receiver_t<server_side>>(&server_data_signal_
                        , server_receivers_epoll_, &server_receivers_ctrl_
                        , &memory_waiter_, &conveyer_, message_f);
        }
        unsigned reader_number = 0;
        for(auto &uptr : clients_senders_){
            uptr = std::make_unique<sender_t<clients_side>>(reader_number++
                        , &clients_data_signal_, &clients_senders_ctrl_
                        , &memory_waiter_, &conveyer_, message_f);
        }
        reader_number = 0;
        for(auto &uptr : server_senders_){
            uptr = std::make_unique<sender_t<server_side>>(reader_number++
                        , &server_data_signal_, &server_senders_ctrl_
                        , &memory_waiter_, &conveyer_, message_f);
        }
        clients_senders_hlp_ = std::make_unique<senders_helper_t<clients_side>>(&clients_data_signal_
                        , clients_senders_epoll_, &clients_senders_ctrl_
                        , &memory_waiter_, &conveyer_, message_f);
        server_senders_hlp_ = std::make_unique<senders_helper_t<server_side>>(&server_data_signal_
                        , server_senders_epoll_, &server_senders_ctrl_
                        , &memory_waiter_, &conveyer_, message_f);
        reader_number = 0;
        for(auto &uptr : clients_loggers_){
            uptr = std::make_unique<psql_logger_t<clients_side>>(reader_number++
                        , &clients_data_signal_, &clients_loggers_ctrl_
                        , &memory_waiter_, &conveyer_, message_f);
        }
        reader_number = 0;
        for(auto &uptr : server_loggers_){
            uptr = std::make_unique<logger_t<server_side>>(reader_number++
                        , &server_data_signal_, &server_loggers_ctrl_
                        , &memory_waiter_, &conveyer_, message_f);
        }
        vector<task_control_t *> memory_consumers_ctrls
                = { &connectors_ctrl_, &clients_receivers_ctrl_ , &server_receivers_ctrl_};
        vector<task_control_t *> memory_producers_ctrls
                = { &clients_senders_ctrl_, &server_senders_ctrl_
                    , &clients_loggers_ctrl_, &server_loggers_ctrl_ };
        vector<task_t *> memory_consumers(connectors_.size() + clients_receivers_.size()
                                   + server_receivers_.size());
        const auto uptr_to_ptr = [](const unique_ptr<task_t> &uptr){ return uptr.get(); };
        auto p = std::transform(connectors_.begin(), connectors_.end()
                                , memory_consumers.begin(), uptr_to_ptr);
        std::transform(clients_receivers_.begin(), clients_receivers_.end(), p, uptr_to_ptr);
        std::transform(server_receivers_.begin(), server_receivers_.end(), p, uptr_to_ptr);
        vector<task_t *> memory_producers(clients_senders_.size() + server_senders_.size()
                                   + clients_loggers_.size() + server_loggers_.size());
        p = std::transform(clients_senders_.begin(), clients_senders_.end()
                           , memory_producers.begin(), uptr_to_ptr);
        p = std::transform(server_senders_.begin(), server_senders_.end(), p, uptr_to_ptr);
        p = std::transform(clients_loggers_.begin(), clients_loggers_.end(), p, uptr_to_ptr);
        std::transform(server_loggers_.begin(), server_loggers_.end(), p, uptr_to_ptr);
        superviser_ = std::make_unique<superviser_t>(std::move(memory_consumers_ctrls)
                    , std::move(memory_producers_ctrls), std::move(memory_consumers)
                    , std::move(memory_producers), &superviser_ctrl_, &memory_waiter_
                    , &conveyer_, &pager_, message_f);
    }

    ~proxy_t() {
        try{ stop(); } catch(...){}
        for_all_epolls([this](int fd){
            close(message_on_error, fd);
        });
    }

    void start() {
        assert(threads_.empty() && conveyer_.peers_count() == 0);
        show_message("starting proxy...");
        std::string msg = "memory cache: ";
        msg += std::to_string(pager_.page_size() * pager_.cache_size());
        msg += " bytes";
        show_message(msg.c_str());
        auto tl = std::to_string(threading_level_);
        msg = "threading level: " + tl + " connectors + "
                + tl + " client receivers + " + tl + " server receivers + "
                + tl + " client senders + " + tl + " server senders + "
                + tl + " client loggers + " + tl + " server loggers";
        show_message(msg.c_str());
        listening_socket_ = socket(throw_on_error, PF_INET
                                   , SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP);
        int opt = 1;
        setsockopt(listening_socket_, SOL_SOCKET, SO_REUSEPORT, (const char*)&opt, sizeof(opt));
        bind(throw_on_error, listening_socket_, (sockaddr *)&proxy_addr_, sizeof(sockaddr_in));
        listen(throw_on_error, listening_socket_, 128);
        epoll_event ee;
        ee.events = EPOLLIN | EPOLLEXCLUSIVE;
        ee.data.fd = listening_socket_;
        epoll_ctl(throw_on_error, connectors_epoll_, EPOLL_CTL_ADD, listening_socket_, &ee);
        threads_.reserve(threading_level_ * 7 + 3);
        const auto run_task = [this](task_t *ptask){
            try{
                ptask->run();
            } catch(std::exception &e){
                show_message("critical exception: ");
                show_message(e.what());
                error_signal_->notify_all();
            } catch(...){
                show_message("critical exception in background thread");
                error_signal_->notify_all();
            }
        };
        for(auto ptasks : { &connectors_, &clients_receivers_, &server_receivers_
            , &clients_senders_, &server_senders_ , &clients_loggers_, &server_loggers_ }){
            for(auto &uptr : *ptasks){
                threads_.emplace_back([ptask = uptr.get(), run_task](){ run_task(ptask); });
            }
        }
        threads_.emplace_back([ptask = server_senders_hlp_.get(), run_task](){ run_task(ptask); });
        threads_.emplace_back([ptask = clients_senders_hlp_.get(), run_task](){ run_task(ptask); });
        threads_.emplace_back([ptask = superviser_.get(), run_task](){ run_task(ptask); });
        show_message("proxy started");
    }

    void stop() {
        if(threads_.empty()){
            return;
        }
        show_message("stopping...");
        for_all_controls([](task_control_t &ctrl){ ctrl.stop(); });
        for(auto &t : threads_){
            try{
                t.join();
            } catch(std::exception &e){
                show_message(e.what());
            }
        }
        threads_.clear();
        conveyer_.clear();
        memory_waiter_.reset();
        pager_.reset();
        clients_data_signal_.reset();
        server_data_signal_.reset();
        psql_logger_t<clients_side>::reset();
        for_all_controls([](task_control_t &ctrl){ ctrl.reset(); });
        epoll_ctl(message_on_error, connectors_epoll_, EPOLL_CTL_DEL, listening_socket_, nullptr);
        close(message_on_error, listening_socket_);
        show_message("proxy stoped");
    }
};

}

using proxy_nms::proxy_t;
