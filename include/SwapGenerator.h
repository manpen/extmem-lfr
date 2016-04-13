/**
 * @file
 * @brief Swap Descriptor and Results
 * @author Michael Hamann
 * @author Manuel Penschuck
 * @copyright to be decided
 */

#pragma once
#include <random>
#include "Swaps.h"

class SwapGenerator {
public:
    using value_type = SwapDescriptor;

protected:
    const int64_t _number_of_edges_in_graph;
    const int64_t _requested_number_of_swaps;

    int64_t _current_number_of_swaps;
    value_type _current_swap;


    std::default_random_engine  _random_gen;
    std::uniform_int_distribution<edgeid_t> _random_edge;

    using flag_word_t = std::default_random_engine::result_type;
    unsigned int _flag_bits_remaining = 0;
    flag_word_t _flag_bits;

public:
    SwapGenerator(int64_t number_of_swaps, int64_t edges_in_graph)
        : _number_of_edges_in_graph(edges_in_graph)
        , _requested_number_of_swaps(number_of_swaps)
        , _current_number_of_swaps(0)
        , _random_edge(0, edges_in_graph-1)
    {
        assert(_number_of_edges_in_graph > 1);
        ++(*this);
    }

    SwapGenerator(const SwapGenerator &) = default;
    ~SwapGenerator() = default;

//! @name STXXL Streaming Interface
//! @{
    bool empty() const {return _current_number_of_swaps > _requested_number_of_swaps;}
    const value_type & operator*() const {return _current_swap;}

    SwapGenerator& operator++() {
        _current_number_of_swaps++;

        if (!UNLIKELY(_flag_bits_remaining)) {
            _flag_bits_remaining = 8 * sizeof(flag_word_t);
            _flag_bits = _random_gen();
        } else {
            _flag_bits >>= 1;
            --_flag_bits_remaining;
        }

        while (1) {
            // generate two disjoint random edge ids
            edgeid_t e1 = _random_edge(_random_gen);
            edgeid_t e2 = _random_edge(_random_gen);
            if (e1 == e2) continue;

            // direction flag
            bool dir = _flag_bits & 1;

            _current_swap = {e1, e2, dir};
            return *this;
        }
    }
//! @}
};
