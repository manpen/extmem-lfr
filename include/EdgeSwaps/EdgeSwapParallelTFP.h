#pragma once

#include <stxxl/vector>
#include <stxxl/sorter>
#include <stxxl/bits/unused.h>

#include <thread>

#include <defs.h>
#include "Swaps.h"
#include "GenericComparator.h"
#include "TupleHelper.h"

#include "EdgeSwapBase.h"
#include "BoolStream.h"
#include <omp.h>

namespace EdgeSwapParallelTFP {
    struct EdgeLoadRequest {
        edgeid_t eid;
        swapid_t sid;
        unsigned char spos;

        DECL_LEX_COMPARE_OS(EdgeLoadRequest, eid, sid, spos);
    };

    struct DependencyChainEdgeMsg {
        swapid_t swap_id;
        unsigned char spos;
        edge_t edge;

        DECL_LEX_COMPARE_OS(DependencyChainEdgeMsg, swap_id, spos, edge);
    };

    struct DependencyChainSuccessorMsg {
        swapid_t swap_id;
        unsigned char spos;
        swapid_t successor;
        unsigned char successor_spos;

        DECL_LEX_COMPARE_OS(DependencyChainSuccessorMsg, swap_id, spos); // NOTE other fields are not necessary for uniqueness and sorting.
    };

    struct ExistenceRequestMsg {
        edge_t edge;
        swapid_t swap_id;
        bool forward_only;

        ExistenceRequestMsg() { }

        ExistenceRequestMsg(const edge_t &edge_, const swapid_t &swap_id_, const bool &forward_only_) :
              edge(edge_), swap_id(swap_id_),  forward_only(forward_only_) { }

        bool operator< (const ExistenceRequestMsg& o) const {
            return (edge < o.edge || (edge == o.edge && (swap_id > o.swap_id || (swap_id == o.swap_id && forward_only < o.forward_only))));
        }
        DECL_TO_TUPLE(edge, swap_id, forward_only);
        DECL_TUPLE_OS(ExistenceRequestMsg);
    };

    struct ExistenceInfoMsg {
        swapid_t swap_id;
        edge_t edge;
      #ifndef NDEBUG
        bool exists;
      #endif

        ExistenceInfoMsg() { }

        ExistenceInfoMsg(const swapid_t &swap_id_, const edge_t &edge_, const bool &exists_ = true) :
            swap_id(swap_id_), edge(edge_)
            #ifndef NDEBUG
            , exists(exists_)
            #endif
        { stxxl::STXXL_UNUSED(exists_); }

        DECL_LEX_COMPARE_OS(ExistenceInfoMsg, swap_id, edge
            #ifndef NDEBUG
            , exists
            #endif
        );
    };

    struct ExistenceSuccessorMsg {
        swapid_t swap_id;
        edge_t edge;
        swapid_t successor;

        ExistenceSuccessorMsg() { }

        ExistenceSuccessorMsg(const swapid_t &swap_id_, const edge_t &edge_, const swapid_t &successor_) :
              swap_id(swap_id_), edge(edge_), successor(successor_) { }

        DECL_LEX_COMPARE_OS(ExistenceSuccessorMsg, swap_id, edge, successor);
    };

    class EdgeSwapParallelTFP : public EdgeSwapBase {
    protected:
        constexpr static size_t _pq_mem = PQ_INT_MEM;
        constexpr static size_t _pq_pool_mem = PQ_POOL_MEM;
        constexpr static size_t _sorter_mem = SORTER_MEM;

        constexpr static bool compute_stats = false;
        constexpr static bool produce_debug_vector=true;

        edge_vector &_edges;
        uint_t _num_swaps_per_iteration;
        swapid_t _swap_id;

// swap direction information
        using BoolVector = stxxl::vector<bool>;
        std::vector<std::unique_ptr<BoolVector>> _swap_direction;
        std::vector<std::unique_ptr<BoolVector::bufwriter_type>> _swap_direction_writer;


// swap -> edge
        using EdgeLoadRequestSorter = stxxl::sorter<EdgeLoadRequest, GenericComparatorStruct<EdgeLoadRequest>::Ascending>;
        EdgeLoadRequestSorter _edge_swap_sorter;

// dependency chain
        // we need to use a desc-comparator since the pq puts the largest element on top
        using DependencyChainEdgeComparatorSorter = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Ascending;
        using DependencyChainEdgeSorter = stxxl::sorter<DependencyChainEdgeMsg, DependencyChainEdgeComparatorSorter>;

        using DependencyChainSuccessorComparator = typename GenericComparatorStruct<DependencyChainSuccessorMsg>::Ascending;
        using DependencyChainSuccessorSorter = stxxl::sorter<DependencyChainSuccessorMsg, DependencyChainSuccessorComparator>;

        using EdgeIdVector = stxxl::VECTOR_GENERATOR<edgeid_t>::result;
        EdgeIdVector _used_edge_ids;

// existence requests
        using ExistenceRequestComparator = typename GenericComparatorStruct<ExistenceRequestMsg>::Ascending;
        using ExistenceRequestSorter = stxxl::sorter<ExistenceRequestMsg, ExistenceRequestComparator>;
        using ExistenceRequestMerger = ExistenceRequestSorter::runs_merger_type;

// existence information and dependencies
        using ExistenceInfoComparator = typename GenericComparatorStruct<ExistenceInfoMsg>::Ascending;
        using ExistenceInfoSorter = stxxl::sorter<ExistenceInfoMsg, ExistenceInfoComparator>;

        using ExistenceSuccessorComparator = typename GenericComparatorStruct<ExistenceSuccessorMsg>::Ascending;
        using ExistenceSuccessorSorter = stxxl::sorter<ExistenceSuccessorMsg, ExistenceSuccessorComparator>;

// edge updates
        using EdgeUpdateComparator = typename GenericComparator<edge_t>::Ascending;
        using EdgeUpdateSorter = stxxl::sorter<edge_t, EdgeUpdateComparator>;
        EdgeUpdateSorter _edge_update_sorter;

// threads
        int _num_threads;

        void _thread(swapid_t swap_id) {
            return swap_id % _num_threads;
        };

// algos
        void _load_and_update_edges(std::vector<std::unique_ptr<DependencyChainEdgeSorter>>& edge_output, std::vector<std::unique_ptr<DependencyChainSuccessorSorter>>& dependency_output);
        void _compute_conflicts(std::vector<std::unique_ptr<DependencyChainEdgeSorter>>& edges, std::vector<std::unique_ptr<DependencyChainSuccessorSorter>>& dependencies, ExistenceRequestMerger& requestOutputMerger);
        void _process_existence_requests();
        void _perform_swaps();

    public:
        EdgeSwapParallelTFP() = delete;
        EdgeSwapParallelTFP(const EdgeSwapParallelTFP &) = delete;

        //! Swaps are performed during constructor.
        //! @param edges  Edge vector changed in-place
        //! @param swaps  Read-only swap vector - ignored!
        EdgeSwapParallelTFP(edge_vector &edges, swap_vector &, uint64_t swaps_per_iteration) :
              EdgeSwapParallelTFP(edges, swaps_per_iteration) { }

        EdgeSwapParallelTFP(edge_vector &edges, uint64_t swaps_per_iteration, int num_threads = omp_get_max_threads()) :
              EdgeSwapBase(),
              _edges(edges),
              _num_swaps_per_iteration(swaps_per_iteration),
              _swap_id(0),

              _swap_direction(num_threads),
              _swap_direction_writer(num_threads),
              _edge_swap_sorter(GenericComparatorTuple<EdgeSwapMsg>::Ascending(), _sorter_mem),
              _edge_update_sorter(EdgeUpdateComparator{}, _sorter_mem),
              _num_threads(num_threads) {
                for (int i = 0; i < _num_threads; ++i) {
                    _swap_direction[i].reset(new BoolVector);
                    _swap_direction[i]->resize(swaps_per_iteration);
                    _swap_direction_writer[i].reset(new BoolVector::bufwriter_type(*_swap_direction[i]));
                }

              } // FIXME actually _edge_update_sorter isn't needed all the time. If memory is an issue, we could safe memory here

        void process_swaps();
        void run();

        void push(const swap_descriptor& swap) {
            _edge_swap_sorter.push(EdgeLoadRequest {swap.edges()[0], 0, _swap_id});
            _edge_swap_sorter.push(EdgeLoadRequest {swap.edges()[1], 1, _swap_id});
            *(_swap_direction_writer[_thread(_swap_id)]) << swap.direction();
            ++_swap_id;
            if (_swap_id >= _num_swaps_per_iteration) {
                process_swaps();
            }
        }
    };
}

template <>
struct EdgeSwapTrait<EdgeSwapParallelTFP::EdgeSwapParallelTFP> {
    static bool swapVector() {return false;}
    static bool pushableSwaps() {return true;}
    static bool pushableSwapBuffers() {return false;}
};