#pragma once

#include <stxxl/vector>
#include <stxxl/priority_queue>
#include <stxxl/sorter>

#include <defs.h>
#include "Swaps.h"
#include "GenericComparator.h"
#include "TupleHelper.h"

#include "EdgeSwapBase.h"

namespace EdgeSwapTFP {
    struct DependencyChainEdgeMsg {
        swapid_t swap_id;
        edgeid_t edge_id;
        edge_t edge;

        DependencyChainEdgeMsg() { }

        DependencyChainEdgeMsg(const swapid_t &swap_id_, const edgeid_t &edge_id_, const edge_t &edge_)
              : swap_id(swap_id_), edge_id(edge_id_), edge(edge_) { }

        DECL_LEX_COMPARE_OS(DependencyChainEdgeMsg, swap_id, edge_id, edge);
    };

    struct DependencyChainSuccessorMsg {
        swapid_t swap_id;
        edgeid_t edge_id;
        swapid_t successor;

        DependencyChainSuccessorMsg() { }

        DependencyChainSuccessorMsg(const swapid_t &swap_id_, const edgeid_t &edge_id_, const swapid_t &successor_) :
              swap_id(swap_id_), edge_id(edge_id_), successor(successor_) { }

        DECL_LEX_COMPARE_OS(DependencyChainSuccessorMsg, swap_id, edge_id, successor);
    };

    struct ExistenceRequestMsg {
        edge_t edge;
        swapid_t swap_id;

        ExistenceRequestMsg() { }

        ExistenceRequestMsg(const edge_t &edge_, const swapid_t &swap_id_) :
              edge(edge_), swap_id(swap_id_) { }

        DECL_LEX_COMPARE_OS(ExistenceRequestMsg, edge, swap_id);
    };

    struct ExistenceInfoMsg {
        swapid_t swap_id;
        swapid_t sender_id;
        edge_t edge;
        bool exists;

        ExistenceInfoMsg() { }

        ExistenceInfoMsg(const swapid_t &swap_id_, const swapid_t &sender_id_, const edge_t &edge_, const bool &exists_) :
              swap_id(swap_id_), sender_id(sender_id_), edge(edge_), exists(exists_) { }

        DECL_LEX_COMPARE_OS(ExistenceInfoMsg, swap_id, sender_id, edge, exists);
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

    struct EdgeUpdateMsg {
        edgeid_t edge_id;
        swapid_t sender;
        edge_t updated_edge;

        EdgeUpdateMsg() { }

        EdgeUpdateMsg(const edgeid_t &edge_id_, const swapid_t &sender_, const edge_t &updated_edge_) :
              edge_id(edge_id_), sender(sender_), updated_edge(updated_edge_) { }

        DECL_LEX_COMPARE_OS(EdgeUpdateMsg, edge_id, sender, updated_edge);
    };

    template<class EdgeVector = stxxl::vector<edge_t>, class SwapVector = stxxl::vector<SwapDescriptor>, bool compute_stats = false>
    class EdgeSwapTFP : public EdgeSwapBase {
    public:
        using debug_vector = stxxl::vector<SwapResult>;
        using edge_vector = EdgeVector;
        using swap_vector = SwapVector;

    protected:
        constexpr static size_t _pq_mem = PQ_INT_MEM;
        constexpr static size_t _pq_pool_mem = PQ_POOL_MEM;
        constexpr static size_t _sorter_mem = SORTER_MEM;

        constexpr static bool _deduplicate_before_insert = true;

        EdgeVector &_edges;
        SwapVector &_swaps;

        debug_vector _result;

// dependency chain
        // we need to use a desc-comparator since the pq puts the largest element on top
        using DependencyChainEdgeComparatorPQ = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Descending;
        using DependencyChainEdgePQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<DependencyChainEdgeMsg, DependencyChainEdgeComparatorPQ, _pq_mem, 1 << 20>::result;
        using DependencyChainEdgePQBlock = typename DependencyChainEdgePQ::block_type;
        using DependencyChainEdgeComparatorSorter = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Ascending;
        using DependencyChainEdgeSorter = stxxl::sorter<DependencyChainEdgeMsg, DependencyChainEdgeComparatorSorter>;
        DependencyChainEdgeSorter _depchain_edge_sorter;

        using DependencyChainSuccessorComparator = typename GenericComparatorStruct<DependencyChainSuccessorMsg>::Ascending;
        using DependencyChainSuccessorSorter = stxxl::sorter<DependencyChainSuccessorMsg, DependencyChainSuccessorComparator>;
        DependencyChainSuccessorSorter _depchain_successor_sorter;

// existence requests
        using ExistenceRequestComparator = typename GenericComparatorStruct<ExistenceRequestMsg>::Ascending;
        using ExistenceRequestSorter = stxxl::sorter<ExistenceRequestMsg, ExistenceRequestComparator>;
        ExistenceRequestSorter _existence_request_sorter;

// existence information and dependencies
        // we need to use a desc-comparator since the pq puts the largest element on top
        using ExistenceInfoComparator = typename GenericComparatorStruct<ExistenceInfoMsg>::Descending;
        using ExistenceInfoPQ = typename
        stxxl::PRIORITY_QUEUE_GENERATOR<ExistenceInfoMsg, ExistenceInfoComparator, _pq_mem, 1 << 20>::result;
        using ExistenceInfoPQBlock = typename ExistenceInfoPQ::block_type;
        stxxl::read_write_pool<ExistenceInfoPQBlock> _existence_info_pool;
        ExistenceInfoPQ _existence_info_pq;

        using ExistenceSuccessorComparator = typename GenericComparatorStruct<ExistenceSuccessorMsg>::Ascending;
        using ExistenceSuccessorSorter = stxxl::sorter<ExistenceSuccessorMsg, ExistenceSuccessorComparator>;
        ExistenceSuccessorSorter _existence_successor_sorter;

// edge updates
        using EdgeUpdateComparator = typename GenericComparatorStruct<EdgeUpdateMsg>::Ascending;
        using EdgeUpdateSorter = stxxl::sorter<EdgeUpdateMsg, EdgeUpdateComparator>;
        EdgeUpdateSorter _edge_update_sorter;

// algos
        void _compute_dependency_chain();
        void _compute_conflicts();
        void _process_existence_requests();
        void _perform_swaps();
        void _apply_updates();

    public:
        EdgeSwapTFP() = delete;

        EdgeSwapTFP(const EdgeSwapTFP &) = delete;

        //! Swaps are performed during constructor.
        //! @param edges  Edge vector changed in-place
        //! @param swaps  Read-only swap vector
        EdgeSwapTFP(edge_vector &edges, swap_vector &swaps) :
              EdgeSwapBase(),
              _edges(edges),
              _swaps(swaps),

              _depchain_edge_sorter(DependencyChainEdgeComparatorSorter(), _sorter_mem),
              _depchain_successor_sorter(DependencyChainSuccessorComparator{}, _sorter_mem),
              _existence_request_sorter(ExistenceRequestComparator{}, _sorter_mem),
              _existence_info_pool(_pq_pool_mem / 2 / ExistenceInfoPQBlock::raw_size, _pq_pool_mem / 2 / ExistenceInfoPQBlock::raw_size),
              _existence_info_pq(_existence_info_pool),
              _existence_successor_sorter(ExistenceSuccessorComparator{}, _sorter_mem),
              _edge_update_sorter(EdgeUpdateComparator{}, _sorter_mem) { }

        void run();

        //! The i-th entry of this vector corresponds to the i-th
        //! swap provided to the constructor
        debug_vector &debugVector() {
           return _result;
        }
    };
}
