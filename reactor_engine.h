#pragma once

#include "constants.h"
#include "types.h"

#include <vector>
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

    std::vector<ClientSession> clientSession;
    std::vector<std::uint32_t> fdsToSlot;
    std::vector<std::uint32_t> freeSlots;


    void acceptNewClients();
    void disconnectClients(int fd);
    void broadcastMessage(int sender_fd, std::string_view msg);
    void handleClient(int fd);
    std::size_t parseBuffer(int sender_fd, std::uint8_t* data, std::size_t dataSize); 
    void handleMessage(int sender_fd, const uint8_t* payload, std::size_t sizeOfPayload);
};