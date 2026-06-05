#pragma once
#include <cstdint>
#include <sys/types.h>
#include <memory>
#include <vector>
#include <cstdint>

namespace RLimitDefaults {
    inline static constexpr std::size_t fallBackDefault = 1024;  
};

struct ClientSession {
    std::uint32_t uniqueID;
    std::int32_t sfd{-1};
    std::unique_ptr<std::vector<std::uint8_t>> pending;  
    bool isActive{false};
};


