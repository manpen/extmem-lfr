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

using node_t = int_t; ///< Type for every node id used in this project
using edge_t = std::pair<node_t, node_t>; ///<Type for every (un)directed edge
using edgeid_t = int_t; ///< Type used to address edges

struct EdgeComparator {
    bool operator()(const edge_t &a, const edge_t &b) const {return a < b;}
    edge_t min_value() const {return std::make_pair(std::numeric_limits<node_t>::min(), std::numeric_limits<node_t>::min());}
    edge_t max_value() const {return std::make_pair(std::numeric_limits<node_t>::max(), std::numeric_limits<node_t>::max());}
};

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

constexpr uint_t SORTER_MEM = 1 * IntScale::Gi; // default bytes used for interal storage of  sorter
constexpr uint_t PQ_INT_MEM = 1 * IntScale::Gi; // default bytes used for internal storage of a PQ
constexpr uint_t PQ_POOL_MEM = 1 * IntScale::Gi; // default bytes used for internal storage of a PQ
