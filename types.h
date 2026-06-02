#pragma once
#include <cstdint>
#include <sys/types.h>

struct ClientSession {

    std::int32_t sfd{-1};
    std::uint8_t readBuffer[4096];    
    ssize_t bytesInBuffer{};
    bool isActive{false};
};


