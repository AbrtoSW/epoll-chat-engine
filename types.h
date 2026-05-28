#pragma once
#include <cstdint>

struct ClientSession {

    std::int32_t sfd{-1};
    std::uint8_t readBuffer[4096];    
    std::size_t  bytesInBuffer{};
    bool isActive{false};
};


