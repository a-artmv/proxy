#pragma once

#include <netinet/in.h>

#include "../exceptions/exceptor.h"
#include "../system/epoller.h"

namespace conveyer_nms { class transfer_conveyer_t; }

namespace connector_nms {

using conveyer_nms::transfer_conveyer_t;

class connector_t : public exceptor_t, protected epoller_t<1> {
    transfer_conveyer_t *conveyer_;
    sockaddr_in server_addr_;
    int clients_receivers_epoll_;
    int server_receivers_epoll_;
    int clients_senders_epoll_;
    int server_senders_epoll_;

    void add_peer(int sock);

public:

    connector_t(const sockaddr_in &server_addr, int connectors_epoll, int clients_receivers_epoll
                , int server_receivers_epoll, int clients_senders_epoll, int server_senders_epoll
                , task_control_t *ctrl, resource_waiter_t *memory_waiter, transfer_conveyer_t *conveyer
                , const std::function<void (const char *)> &message_f)
        : exceptor_t(memory_waiter, ctrl, message_f)
        , epoller_t(connectors_epoll, max_response, message_f
                    , [this](operation_t op){ return except(op); })
        , conveyer_(conveyer), server_addr_(server_addr)
        , clients_receivers_epoll_(clients_receivers_epoll)
        , server_receivers_epoll_(server_receivers_epoll)
        , clients_senders_epoll_(clients_senders_epoll)
        , server_senders_epoll_(server_senders_epoll) {}

    const char *name() const override { return "connector"; }

protected:

    bool one_step() override {
        return epoll([this](int sock){ add_peer(sock); }) != -1;
    }

    bool on_start() override {
        show_message("connector thread started");
        return true;
    }

    void on_finish() override {
        show_message("connector thread finished");
    }
};

}

using connector_nms::connector_t;
