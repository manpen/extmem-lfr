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
#include <Utils/RandomBoolStream.h>

class SwapGenerator {
public:
    using value_type = SwapDescriptor;

protected:
    const int64_t _number_of_edges_in_graph;
    const int64_t _requested_number_of_swaps;

    int64_t _current_number_of_swaps;
    value_type _current_swap;

    STDRandomEngine _rand_gen;
    std::uniform_int_distribution<edgeid_t> _random_integer;

    RandomBoolStream _bool_stream;

public:
    SwapGenerator(int64_t number_of_swaps, int64_t edges_in_graph, uint32_t seed)
        : _number_of_edges_in_graph(edges_in_graph)
        , _requested_number_of_swaps(number_of_swaps)
        , _current_number_of_swaps(0)
        , _rand_gen(seed)
        , _random_integer(0, _number_of_edges_in_graph-1)
        , _bool_stream(_rand_gen())
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

        ++_bool_stream;

        while (1) {
            // generate two disjoint random edge ids
            edgeid_t e1 = _random_integer(_rand_gen);
            edgeid_t e2 = _random_integer(_rand_gen);
            if (e1 == e2) continue;

            // direction flag
            bool dir = *_bool_stream;

            _current_swap = {e1, e2, dir};
            return *this;
        }
    }
//! @}
};
