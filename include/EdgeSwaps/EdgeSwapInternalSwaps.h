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
#include "TupleHelper.h"
#include <algorithm>
#include <iterator>

class EdgeSwapInternalSwaps : public EdgeSwapBase {
protected:
    edge_vector & _edges;
#ifdef EDGE_SWAP_DEBUG_VECTOR
    typename debug_vector::bufwriter_type _debug_vector_writer;
#endif

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
    void performSwaps();
    void updateEdgesAndLoadSwapsWithEdgesAndSuccessors();

    void process_buffer();

public:
    EdgeSwapInternalSwaps() = delete;
    EdgeSwapInternalSwaps(const EdgeSwapInternalSwaps &) = delete;


    //! Swaps are performed during constructor.
    //! @param edges  Edge vector changed in-place
    EdgeSwapInternalSwaps(edge_vector & edges, int_t num_swaps_per_iteration = 1000000) :
        EdgeSwapBase()
        , _edges(edges)
#ifdef EDGE_SWAP_DEBUG_VECTOR
        , _debug_vector_writer(_result)
#endif
        , _num_swaps_per_iteration(num_swaps_per_iteration)
        , _query_sorter(typename GenericComparatorStruct<edge_existence_request_t>::Ascending(), SORTER_MEM)
    {
        _current_swaps.reserve(_num_swaps_per_iteration);
    }

    //! Swaps are performed during constructor.
    //! @param edges  Edge vector changed in-place
    //! @param swaps  IGNORED - use push interface
    EdgeSwapInternalSwaps(edge_vector & edges, swap_vector &, int_t num_swaps_per_iteration = 1000000) :
        EdgeSwapInternalSwaps(edges, num_swaps_per_iteration) {}

    //! Push a single swap into buffer; if buffer overflows, all stored swap are processed
    void push(const swap_descriptor& swap) {
        _current_swaps.push_back(swap);
        if (UNLIKELY(static_cast<int_t>(_current_swaps.size()) >= _num_swaps_per_iteration)) {
            process_buffer();
        }
    }

    //! Takes ownership of buffer provided and returns old internal buffer which
    //! is guranteed to be empty.
    //! @warning If old buffer contains data, it is processed which may be very
    //! in case the buffer is not full yet
    void swap_buffer(std::vector<swap_descriptor> & buffer) {
        if (!_current_swaps.empty()) {
            process_buffer();
        }

        _current_swaps.swap(buffer);
    }

    //! Processes buffered swaps and writes out changes
    void run() {
        if (!_current_swaps.empty())
            process_buffer();

        updateEdgesAndLoadSwapsWithEdgesAndSuccessors();
#ifdef EDGE_SWAP_DEBUG_VECTOR
        _debug_vector_writer.finish();
#endif
    }
};

template <>
struct EdgeSwapTrait<EdgeSwapInternalSwaps> {
    static bool swapVector() {return false;}
    static bool pushableSwaps() {return true;}
    static bool pushableSwapBuffers() {return true;}
};