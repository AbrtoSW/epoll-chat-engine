#include "reactor_engine.h"

#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <cstring>
#include <print>
#include <sys/resource.h>

bool ReactorEngine::init() {

    listener_fd = socket(AF_INET6, SOCK_STREAM, 0);

    if (listener_fd < 0) {
        std::cerr << "could not init socket";   
        return false;
    }

    int opt_disable{0};
    int opt_enable{1};

    if (setsockopt(listener_fd, IPPROTO_IPV6, IPV6_V6ONLY,  &opt_disable, sizeof(opt_disable)) < 0){
        std::cerr << "could not set socket options for ipv6 flexability with ipv4" << std::endl;
        return false;
    };

    // disable this once testing is done 
    if (setsockopt(listener_fd, SOL_SOCKET, SO_REUSEADDR,  &opt_enable, sizeof(opt_enable)) < 0){
        std::cerr << "could not set reuse addr setting" << std::endl;
        return false;
    };
    

    int flags = fcntl(listener_fd, F_GETFL, 0);

    if (flags < 0) {
        std::cerr << "failed to retrieve fd flags\n";
        return false;
    }

    if (fcntl(listener_fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        std::cerr << "failed to set non block flags on the listener fd";
        return false;
    }

    sockaddr_in6 socketAddr{};
    socketAddr.sin6_family = AF_INET6;
    socketAddr.sin6_port = htons(8080);
    socketAddr.sin6_addr = in6addr_any;


    if (bind(listener_fd,reinterpret_cast<struct sockaddr*>(&socketAddr), sizeof(socketAddr)) < 0) {
        std::cerr << "could not bind port 8080 is taken" << std::endl;
        return false;
    }

    if (listen(listener_fd, 128) < 0) {
        std::cerr << "could not listen to connections";
        return false;
    }

    epoll_fd = epoll_create1(0);

    if (epoll_fd < 0) {
          std::cerr << "could not create epoll instance\n";
        return false;
    }

    epoll_event ev;
    // might add epollet later 
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listener_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listener_fd, &ev) < 0) {
        std::cerr << "could not initalize epoll\n"; 
        return false;
    }

    initializeDataHolders();

    isActive = true;
    return true;
}



void ReactorEngine::run() {

    constexpr std::uint8_t MAX_EVENTS{255};
    epoll_event e_event[MAX_EVENTS];

    while (isActive) {

        int numEvents = epoll_wait(epoll_fd, e_event, MAX_EVENTS, -1);

        for (int i = 0; i < numEvents; ++i) {
            int eventTriggered = e_event[i].data.fd;
            std::uint32_t eventType = e_event[i].events;

            if (eventTriggered == listener_fd) {
                if (eventType & EPOLLIN) {
                    acceptNewClients();
                }
                continue;
            } 

            if (eventType & (EPOLLHUP | EPOLLERR)) {
                disconnectClients(eventTriggered);
                continue;
            }

            if (eventType & EPOLLIN) {
                handleClient(eventTriggered);
            }
            


        }
        
    }
    
}


void ReactorEngine::acceptNewClients() {

    // this is to store info like ip and port 
    struct sockaddr_storage clientAddr;
    socklen_t addr_size = sizeof(clientAddr);
    
    int sfd{};

    while ((sfd = accept(listener_fd, (struct sockaddr *)&clientAddr, &addr_size)) >= 0) {

        int flags = fcntl(sfd, F_GETFL, 0);
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK);

        if (freeSlots.empty()) {
            std::print("acceptNewClients: accepted fd {} but no free slots available, closing\n", sfd);
            close(sfd);
            continue;
        }

        int freeSlot = freeSlots.back();
        freeSlots.pop_back();

        clientSession[freeSlot].sfd = sfd;
        clientSession[freeSlot].isActive = true;
        fdsToSlot[sfd] = freeSlot;

        std::print("acceptNewClients: accepted fd {} mapped to slot {}, freeSlots={}\n", sfd, freeSlot, freeSlots.size());

        struct epoll_event ev;

        std::memset(&ev, 0, sizeof(ev));

        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = sfd;

        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sfd, &ev) == -1) {
            perror("epoll_ctl: EPOLL_CTL_ADD failed");
        
            clientSession[freeSlot].isActive = false;
            clientSession[freeSlot].sfd = -1;
            fdsToSlot[sfd] = -1;
            freeSlots.push_back(freeSlot);
            close(sfd);
        }

    }

}

void ReactorEngine::handleClient(int fd) {
    //this will be multhireaded but first testing single thread
    std::uint32_t slot = fdsToSlot[fd];
    if (slot == CONSTANTS::INVALID_SLOT) {
        std::print("handleClient: fd {} has invalid slot\n", fd);
        return;
    }

    ClientSession& client = clientSession[slot];
    if (!client.isActive || client.sfd != fd) {
        std::print("handleClient: fd {} mapped to slot {} but session inactive or sfd mismatch (session.sfd={})\n", fd, slot, client.sfd);
        disconnectClients(fd);
        return;
    }

    if (client.isActive) {

        while (true) {

            std::uint8_t readBuffer[4096];
            ssize_t bytesInBuffer = recv(client.sfd, readBuffer, sizeof(readBuffer), 0);

            if (bytesInBuffer > 0) {

                if (client.pending && !client.pending->empty()) {

                    client.pending->insert(client.pending->end(), readBuffer, readBuffer + bytesInBuffer);
                    std::size_t bytesProcessed = parseBuffer(client, *client.pending->data(), client.pending->size());

                    if (bytesProcessed > 0) {
                    // Erase processed bytes from the front of the vector
                        client.pending->erase(client.pending->begin(), client.pending->begin() + bytesProcessed);
                    }

                } else {

                    std::size_t bytesProccessed = parseBuffer(client, *readBuffer, bytesInBuffer);

                    if (bytesProccessed < static_cast<std::size_t>(bytesInBuffer)) {

                        if (!client.pending) {
                            client.pending = std::make_unique<std::vector<std::uint8_t>>();
                        }

                        client.pending->insert(client.pending->end(), readBuffer + bytesProccessed, readBuffer + bytesInBuffer);
                    }

                    //either make the printing inside the parser or out here see which is cleaner 

                }

                

            } else if (bytesInBuffer == 0) {
                disconnectClients(client.sfd);
                break;
            } else {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    break;
                    // fake error
                    // should i disconnct?
                } else {
                    std::print("REAL ERROR CODE: {}", errno);
                    //printf("real error\n");
                    break;
                }
            }

        }

    }

}


void ReactorEngine::disconnectClients(int fd){

    int slot = fdsToSlot[fd];
    std::print("disconnecting fd {} slot {}\n", fd, slot);
    if (slot == CONSTANTS::INVALID_SLOT) {
        std::print("disconnectClients: fd {} already invalid\n", fd);
        return;
    }
    if (clientSession[slot].sfd != fd) {
        std::print("disconnectClients: fd {} mapped to slot {} but session.sfd={}\n", fd, slot, clientSession[slot].sfd);
    }

    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    clientSession[slot].isActive = false;
    clientSession[slot].sfd = -1;
    fdsToSlot[fd] = CONSTANTS::INVALID_SLOT;
    freeSlots.push_back(slot);
    std::print("disconnectClients: fd {} slot {} cleaned up, freeSlots={}\n", fd, slot, freeSlots.size());

}

void ReactorEngine::initializeDataHolders() {

    struct rlimit rl;
    if(getrlimit(RLIMIT_NOFILE, &rl) < 0) {
        std::cerr << "getrlimit failed\n";
        rl.rlim_cur = RLimitDefaults::fallBackDefault; 
    }

    std::size_t fdMax = rl.rlim_cur;
    std::print("Initializing fdsToSlot for max_fd={}\n", fdMax);


    clientSession.resize(CONSTANTS::MAX_CLIENTS);
    fdsToSlot.assign(fdMax, CONSTANTS::INVALID_SLOT);
    freeSlots.reserve(CONSTANTS::MAX_CLIENTS);

    for (int i = 0; i < CONSTANTS::MAX_CLIENTS; ++i) {
        freeSlots.push_back(i);
    }

}

std::size_t ReactorEngine::parseBuffer(const ClientSession& client, std::uint8_t data, std::size_t dataSize) {
    return -1;
}
