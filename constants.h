#pragma once
#include <cstdint>
    


namespace CONSTANTS {

    constexpr std::size_t MAX_MESSAGE_SIZE = 1024 * 1024;
    static constexpr std::uint32_t INVALID_SLOT = 0xFFFFFFFF; // or just ~0U
    inline static constexpr std::uint32_t MAX_CLIENTS = 5000000;

}