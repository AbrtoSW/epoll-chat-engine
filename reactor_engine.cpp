#include "reactor_engine.h"

#include <iostream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <cstring>


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
    ev.events = EPOLLIN;
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
            int eventType = e_event[i].events;

            if (eventTriggered == listener_fd) {
                if (eventType & EPOLLIN) {
                    acceptNewClients();
                }
            }  else if (eventType & EPOLLIN) {
                //this function might need a return from acceptNewClients or access from the arrays
                //handleClient();
            } else if (eventType & (EPOLLERR | EPOLLHUP) ) {
                //disconnectClients();
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

        int freeSlot = freeSlots.top();
        freeSlots.pop();

        clientSession[freeSlot].sfd = sfd;
        clientSession[freeSlot].isActive = true;
        fdsToSlot[clientSession[freeSlot].sfd] = freeSlot;


        std::cerr << "[DEBUG] accepted sfd=" << sfd
          << " assigned slot=" << freeSlot
          << " mapped fdsToSlot[" << sfd << "]=" << fdsToSlot[sfd]
          << std::endl;
    

        std::cerr << "[DEBUG] session[" << freeSlot << "]: "
          << "sfd=" << clientSession[freeSlot].sfd
          << " isActive=" << clientSession[freeSlot].isActive
          << std::endl;

          if (fdsToSlot[sfd] != freeSlot) {
        std::cerr << "[ERROR] slot mismatch! expected "
              << freeSlot << " got " << fdsToSlot[sfd]
              << std::endl;
        }

    }



}

void ReactorEngine::initializeDataHolders() {


    for (int i = 0; i < CONSTANTS::MAX_FDS; ++i) {
        freeSlots.push(i);
    }

}
