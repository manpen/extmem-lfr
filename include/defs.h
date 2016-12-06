/**
 * @file
 * @brief  Defines all global data types and constants and should be included in every file of the project.
 * @author Michael Hamann
 * @author Manuel Penschuck
 * @copyright to be decided
 */

#pragma once
#include <cstdint>
#include <utility>
#include <limits>
#include <ostream>
#include <stxxl/bits/common/uint_types.h>

#ifndef SEQPAR
    #if 1
        #include <parallel/algorithm>
        #define SEQPAR __gnu_parallel
    #else
        #define SEQPAR std
    #endif
#endif

/** 
 * @typedef int_t
 * @brief The default signed integer to be used.
 * 
 * You can assume, that this type has always at least 64 bit
 */
using int_t = std::int64_t;

/** 
 * @typedef uint_t
 * @brief The default unsigned integer to be used.
 *
 * You can assume, that this type has always at least 64 bit
 */
using uint_t = std::uint64_t;

//using node_t = int_t; ///< Type for every node id used in this project
using node_t = int64_t;
constexpr node_t INVALID_NODE = std::numeric_limits<node_t>::max();

using degree_t = int32_t; ///< Type for node degrees
using edgeid_t = int_t; ///< Type used to address edges
using community_t = int32_t; ///< Type used to address communities
//Hung
using multinode_t = int64_t;
constexpr multinode_t INVALID_MULTINODE = std::numeric_limits<multinode_t>::max();
using edge64id_t = int_t;

//! Type for every (un)directed 64bit
struct edge64_t : public std::pair<multinode_t, multinode_t> {
    edge64_t() : std::pair<multinode_t, multinode_t>() {}
    edge64_t(const std::pair<multinode_t, multinode_t> & edge) : std::pair<multinode_t, multinode_t>(edge) {}
    edge64_t(const multinode_t & v1, const multinode_t & v2) : std::pair<multinode_t, multinode_t>(v1, v2) {}

    static edge64_t invalid() {
        return edge64_t(INVALID_NODE, INVALID_NODE);
    }

    //! Enforces first<=second
    void normalize() {
        if (first > second)
            std::swap(first, second);
    }

    //! Returns true if edge represents a self-loop
    bool is_loop() const {
        return first == second;
    }
};

/*
namespace std {
    template <>
    class numeric_limits<edge_t> {
    public:
        static edge_t min() { return {numeric_limits<node_t>::min(), numeric_limits<node_t>::min()}; }
        static edge_t max() { return {numeric_limits<node_t>::max(), numeric_limits<node_t>::max()}; }
    };
}
inline std::ostream &operator<<(std::ostream &os, const edge64_t & t) {
   os << "edge64(" << t.first << "," << t.second << ")";
   return os;
}*/

//!Type for every (un)directed edge
struct edge_t : public std::pair<node_t, node_t> {
    edge_t() : std::pair<node_t, node_t>() {}
    edge_t(const std::pair<node_t, node_t> & edge) : std::pair<node_t, node_t>(edge) {}
    edge_t(const node_t & v1, const node_t & v2) : std::pair<node_t, node_t>(v1, v2) {}

    static edge_t invalid() {
        return edge_t(INVALID_NODE, INVALID_NODE);
    }

    //! Enforces first<=second
    void normalize() {
        if (first > second)
            std::swap(first, second);
    }

    //! Returns true if edge represents a self-loop
    bool is_loop() const {
        return first == second;
    }

    //! Returns true if edge is equal to edge_t::invalid()
    bool is_invalid() const {
        return (*this == invalid());
    }
};

namespace std {
    template <>
    class numeric_limits<edge64_t> {
    public:
        static edge64_t min() { return {numeric_limits<multinode_t>::min(), numeric_limits<multinode_t>::min()}; }
        static edge64_t max() { return {numeric_limits<multinode_t>::max(), numeric_limits<multinode_t>::max()}; }
    };

    template <>
    class numeric_limits<edge_t> {
    public:
        static edge_t min() { return {numeric_limits<node_t>::min(), numeric_limits<node_t>::min()}; }
        static edge_t max() { return {numeric_limits<node_t>::max(), numeric_limits<node_t>::max()}; }
    };
}
inline std::ostream &operator<<(std::ostream &os, const edge_t & t) {
   os << "edge(" << t.first << "," << t.second << ")";
   return os;
}

/**
 * @class Scale
 * @brief Common constants for scaling
 * 
 * This class is not meant to be used directly; use one of its named specialisations.
 * 
 * @see IntScale
 * @see UIntScale
 * @see DblScale
 */
template <typename T>
struct Scale {
    static constexpr T K = static_cast<T>(1000LLU);   ///< Kilo (base 10)
    static constexpr T M = K * K;                     ///< Mega (base 10)
    static constexpr T G = K * K * K;                 ///< Giga (base 10)
    static constexpr T P = K * K * K * K;             ///< Peta (base 10)
    
    static constexpr T Ki = static_cast<T>(1024LLU);  ///< Kilo (base 2)
    static constexpr T Mi = Ki* Ki;                   ///< Mega (base 2)
    static constexpr T Gi = Ki* Ki* Ki;               ///< Giga (base 2)
    static constexpr T Pi = Ki* Ki* Ki* Ki;           ///< Peta (base 2)
};

/**
 * @typedef IntScale
 * @brief Specialisation of Scale to int_t
 * @see Scale
 */
using IntScale = Scale<int_t>;

/**
 * @typedef UIntScale
 * @brief Specialisation of Scale to uint_t
 * @see Scale
 */
using UIntScale = Scale<int_t>;

/**
 * @typedef DblScale
 * @brief Specialisation of Scale to double
 * @see Scale
 */
using DblScale = Scale<double>;

#ifndef NDEBUG
#define DEBUG_MSG(show, msg) if(show) {std::cout << msg << std::endl;}
#else
#define DEBUG_MSG(show, msg) {}
#endif

constexpr uint_t SORTER_MEM = 2 * IntScale::Gi; // default bytes used for internal storage of  sorter
constexpr uint_t PQ_INT_MEM = 128 * IntScale::Mi; // default bytes used for internal storage of a PQ
constexpr uint_t PQ_POOL_MEM = 128 * IntScale::Mi; // default bytes used for internal storage of a PQ


//constexpr uint_t SORTER_MEM = 512 * IntScale::Mi; // default bytes used for interal storage of  sorter
//constexpr uint_t PQ_INT_MEM = 512 * IntScale::Mi; // default bytes used for internal storage of a PQ
//constexpr uint_t PQ_POOL_MEM = 256 * IntScale::Mi; // default bytes used for internal storage of a PQ
