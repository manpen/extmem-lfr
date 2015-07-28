#pragma once

#include <cstdint>


using int_t = std::int64_t;
using uint_t = std::uint64_t;

using node_t = int_t;
using edge_t = std::pair<node_t, node_t>;

template <typename T>
struct Scale {
    static constexpr T K = static_cast<T>(1000LLU);
    static constexpr T M = K * K;
    static constexpr T G = K * K * K;
    static constexpr T P = K * K * K * K;
    
    static constexpr T Ki = static_cast<T>(1024LLU);
    static constexpr T Mi = Ki* Ki;
    static constexpr T Gi = Ki* Ki* Ki;
    static constexpr T Pi = Ki* Ki* Ki* Ki;
};

using IntScale = Scale<int_t>;
using DblScale = Scale<double>;