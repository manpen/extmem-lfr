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
#include <functional>
#include <EdgeSwaps/EdgeSwapInternalSwapsBase.h>
#include <EdgeStream.h>

class EdgeSwapInternalSwaps : public EdgeSwapInternalSwapsBase {
public:
    using updated_edges_callback_t = std::function<void(const std::vector<edge_t> &)>;
protected:
    EdgeStream & _edges;

    int_t _num_swaps_per_iteration;
    std::vector<swap_descriptor> _current_swaps;
    std::vector<SemiLoadedSwapDescriptor> _current_semiloaded_swaps;
    std::vector<edgeid_t> _edge_ids_in_current_swaps;
    std::vector<edge_t> _edges_in_current_swaps;

    std::array<std::vector<bool>, 2> _swap_has_successor;

    updated_edges_callback_t _updated_edges_callback;

    void updateEdgesAndLoadSwapsWithEdgesAndSuccessors();

public:
    EdgeSwapInternalSwaps() = delete;
    EdgeSwapInternalSwaps(const EdgeSwapInternalSwaps &) = delete;

    static constexpr int_t maxSwaps() {
        return std::numeric_limits<internal_swapid_t>::max();
    };

    static uint_t memoryUsage(int_t numSwaps) {
        return sizeof(swap_descriptor) /* 24 */ * numSwaps + // _current_swaps, needed with random access while loading swaps but afterwards only two scans are needed for simulation and execute
            (sizeof(edgeid_t) /* 8 */ + sizeof(edge_t)) /* 16 */ * numSwaps * 2 +  // _edge_ids_in_current_swaps + _edges_in_current_swaps, first one only written and read once sequentially, edges need random access
            numSwaps/4 + // _swap_has_successor - written with random access while loading, then only read once sequentially (but really small)
            SORTER_MEM + // _query_sorter - needed only while simulating swaps and loading conflicts
            (sizeof(edge_existence_answer_t) /* 32 */ + sizeof(edge_existence_successor_t) /* 32 */ ) * numSwaps/10 + // _edge_existence_pq + _edge_existence_successors (estimated)
            (sizeof(std::vector<edge_t>) /* 24 */ + sizeof(edge_t) /* 16 */ ) * numSwaps * 2  // possibleEdges in simulateSwapsAndGenerateEdgeExistenceQuery()
            ; // TODO: _edge_existence_pq/_edge_existence_successors and possibleEdges don't need to be allocated at the same time.
            // TODO here the internal vectors from updateEdgesAndLoadSwapsWithEdgesAndSuccessors() are not considered. Make sure we de-allocate enough memory so they don't matter.
    };

    //! Swaps are performed during constructor.
    //! @param edges  Edge vector changed in-place
    EdgeSwapInternalSwaps(EdgeStream & edges, int_t num_swaps_per_iteration = 1000000) :
        EdgeSwapInternalSwapsBase()
        , _edges(edges)
        , _num_swaps_per_iteration(num_swaps_per_iteration)
    {
        if (UNLIKELY(num_swaps_per_iteration > std::numeric_limits<internal_swapid_t>::max())) {
            throw std::runtime_error("Error, only 4 billion swaps per iteration are possible!");
        }
        _current_swaps.reserve(_num_swaps_per_iteration);
    }

    //! Swaps are performed during constructor.
    //! @param edges  Edge vector changed in-place
    //! @param swaps  IGNORED - use push interface
    EdgeSwapInternalSwaps(EdgeStream & edges, swap_vector &, int_t num_swaps_per_iteration = 1000000) :
        EdgeSwapInternalSwaps(edges, num_swaps_per_iteration) {}

    void setUpdatedEdgesCallback(updated_edges_callback_t callback) {
        _updated_edges_callback = callback;
    };

    //! Push a single swap into buffer; if buffer overflows, all stored swap are processed
    void push(const swap_descriptor& swap) {
        _current_swaps.push_back(swap);
        if (UNLIKELY(static_cast<int_t>(_current_swaps.size() + _current_semiloaded_swaps.size()) >= _num_swaps_per_iteration)) {
            process_buffer();
        }
    }

    void push(const SemiLoadedSwapDescriptor& swap) {
        _current_semiloaded_swaps.push_back(swap);
        if (UNLIKELY(static_cast<int_t>(_current_swaps.size() + _current_semiloaded_swaps.size()) >= _num_swaps_per_iteration)) {
            process_buffer();
        }
    }

    //! Takes ownership of buffer provided and returns old internal buffer which
    //! is guranteed to be empty.
    //! @warning If old buffer contains data, it is processed which may be very
    //! in case the buffer is not full yet
    void swap_buffer(std::vector<swap_descriptor> & buffer) {
        if (!_current_swaps.empty() || !_current_semiloaded_swaps.empty()) {
            process_buffer();
        }

        _current_swaps.swap(buffer);
    }

    //! Takes ownership of buffer provided and returns old internal buffer which
    //! is guranteed to be empty.
    //! @warning If old buffer contains data, it is processed which may be very
    //! in case the buffer is not full yet
    void swap_buffer(std::vector<SemiLoadedSwapDescriptor> & buffer) {
        if (!_current_swaps.empty() || !_current_semiloaded_swaps.empty()) {
            process_buffer();
        }

        _current_semiloaded_swaps.swap(buffer);
    }

    void process_buffer();

    //! Processes buffered swaps and writes out changes; further swaps can still be supplied afterwards.
    void flush() {
        if (!_current_swaps.empty() || !_current_semiloaded_swaps.empty()) {
            process_buffer();
        }

        updateEdgesAndLoadSwapsWithEdgesAndSuccessors();
    };

    //! Processes buffered swaps and writes out changes; no more swaps can be supplied afterwards.
    void run() {
        flush();
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
    static bool edgeStream() {return true;}
};