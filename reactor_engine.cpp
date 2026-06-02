#include "reactor_engine.h"

#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <cstring>
#include <print>


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
            
             
            if (eventType & (EPOLLERR | EPOLLHUP) ) {
                std::print("Error or Hangup on fd, {}", eventTriggered);
                //disconnectClients();
                continue;
            }

            if (eventType & EPOLLIN) {
                handleClient(eventTriggered);
            }

        }
        
    }
    
}


//acceptNewClients() should ideally detect EAGAIN/EWOULDBLOCK explicitly, not just rely on loop exit.
void ReactorEngine::acceptNewClients() {

    // this is to store info like ip and port 
    struct sockaddr_storage clientAddr;
    socklen_t addr_size = sizeof(clientAddr);
    
    int sfd{};

    while ((sfd = accept(listener_fd, (struct sockaddr *)&clientAddr, &addr_size)) >= 0) {

        int flags = fcntl(sfd, F_GETFL, 0);
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK);


        int freeSlot = freeSlots.top();
        freeSlots.pop();

        clientSession[freeSlot].sfd = sfd;
        clientSession[freeSlot].isActive = true;
        fdsToSlot[sfd] = freeSlot;

        struct epoll_event ev;

        std::memset(&ev, 0, sizeof(ev));

        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = sfd;

        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sfd, &ev) == -1) {
            perror("epoll_ctl: EPOLL_CTL_ADD failed");
        
            clientSession[freeSlot].isActive = false;
            clientSession[freeSlot].sfd = -1;
            fdsToSlot[sfd] = -1;
            freeSlots.push(freeSlot);
            close(sfd);
        }

    }

}

void ReactorEngine::handleClient(int fd) {

    ClientSession& client = clientSession[fdsToSlot[fd]];

    if (client.isActive) {

        while (true) {

            client.bytesInBuffer = recv(client.sfd, client.readBuffer, sizeof(client.readBuffer), 0);

            if (client.bytesInBuffer > 0) {
                // this could possibly change back to a char as client.readbuffer - 1
                std::string_view msg(reinterpret_cast<const char*>(client.readBuffer), client.bytesInBuffer);
                std::print("client said: {}", msg);
                //printf("client said: %s\n");

            } else if (client.bytesInBuffer == 0) {
                std::print("client disconnected gracefully");
                //printf("client disconnected gracefully\n");
                break;
            } else {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    break;
                    // fake error
                } else {
                    std::print("REAL ERROR CODE: {}", errno);
                    //printf("real error\n");
                    break;
                }
            }

        }

    }

}

void ReactorEngine::initializeDataHolders() {


    for (int i = 0; i < CONSTANTS::MAX_FDS; ++i) {
        freeSlots.push(i);
    }

}
