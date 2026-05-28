#pragma once

#include "constants.h"
#include "types.h"

#include <stack>
#include <string_view>
#include <cstdint>

class ReactorEngine {

    public:

    //fix this 
    // ~ReactorEngine() {
    //     if (listener_fd >= 0) close(listener_fd);
    //     if (epoll_fd >= 0)    close(epoll_fd);
    // }

    bool init();
    void initializeDataHolders();
    void run();

    private: 

    int epoll_fd{-1};
    int listener_fd{-1};
    
    bool isActive{};

    ClientSession clientSession[CONSTANTS::MAX_FDS];
    std::uint32_t fdsToSlot[CONSTANTS::MAX_FDS];
    std::stack<std::uint32_t> freeSlots{}; 

    void acceptNewClients();
    void disconnectClients();
    void broadcastMessage(int sender_fd, std::string_view message);
    void handleClient(int fd);
};