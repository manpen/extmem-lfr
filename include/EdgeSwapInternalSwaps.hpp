#pragma once

#include <stxxl/vector>
#include <stxxl/sorter>
#include <tuple>
#include <set>
#include <vector>
#include <stack>
#include <functional>
#include "EdgeSwapBase.h"
#include "GenericComparator.h"
#include <TupleHelper.h>
#include <algorithm>
#include <iterator>

class EdgeSwapInternalSwaps : public EdgeSwapBase {
protected:
    edge_vector & _edges;
    swap_vector & _swaps;

    int_t _num_swaps_per_iteration;
    std::vector<swap_descriptor> _current_swaps;
    std::vector<edgeid_t> _edge_ids_in_current_swaps;
    std::vector<edge_t> _edges_in_current_swaps;

    std::vector<bool> _swap_has_successor[2];

    struct edge_existence_request_t {
        edge_t e;
        int_t sid;
        bool forward_only; // if this requests is only for generating the correct forwaring information but no existence information is needed
        DECL_TO_TUPLE(e, sid, forward_only);
        bool operator< (const edge_existence_request_t& o) const {
            return (e < o.e || (e == o.e && (sid > o.sid || (sid == o.sid && forward_only < o.forward_only))));
        }
    };

    stxxl::sorter<edge_existence_request_t, typename GenericComparatorStruct<edge_existence_request_t>::Ascending> _query_sorter; // Query of possible conflict edges. This may be large (too large...)

    struct edge_existence_answer_t {
        int_t sid;
        edge_t e;
#ifndef NDEBUG
        bool exists;
#endif
        DECL_LEX_COMPARE(edge_existence_answer_t, sid, e);
    };

    std::vector<edge_existence_answer_t> _edge_existence_pq;

    struct edge_existence_successor_t {
        int_t from_sid;
        edge_t e;
        int_t to_sid;
        DECL_LEX_COMPARE(edge_existence_successor_t, from_sid, e);
    };

    std::vector<edge_existence_successor_t> _edge_existence_successors;

    void simulateSwapsAndGenerateEdgeExistenceQuery();
    void loadEdgeExistenceInformation();
    void performSwaps(
#ifdef EDGE_SWAP_DEBUG_VECTOR
        typename debug_vector::bufwriter_type &debug_vector_writer
#endif
    );

    void updateEdgesAndLoadSwapsWithEdgesAndSuccessors(typename swap_vector::bufreader_type &swapReader);

public:
    EdgeSwapInternalSwaps() = delete;
    EdgeSwapInternalSwaps(const EdgeSwapInternalSwaps &) = delete;

    //! Swaps are performed during constructor.
    //! @param edges  Edge vector changed in-place
    //! @param swaps  Read-only swap vector
    EdgeSwapInternalSwaps(edge_vector & edges, swap_vector & swaps, int_t num_swaps_per_iteration = 1000000) :
        EdgeSwapBase(),
        _edges(edges),
        _swaps(swaps),
        _num_swaps_per_iteration(num_swaps_per_iteration),
        _query_sorter(typename GenericComparatorStruct<edge_existence_request_t>::Ascending(), SORTER_MEM)
    {}

    void run();
};

