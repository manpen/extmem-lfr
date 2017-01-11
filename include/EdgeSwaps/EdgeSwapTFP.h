#pragma once

#include <stxxl/vector>
#include <stxxl/sorter>
#include <stxxl/bits/unused.h>
#include <memory>
#include <thread>

#include <defs.h>
#include "Swaps.h"
#include "GenericComparator.h"
#include "TupleHelper.h"

#include "EdgeSwapBase.h"
#include "BoolStream.h"
#include <stxxl/priority_queue>

#include <EdgeStream.h>
#include <Utils/export_metis.h>

namespace EdgeSwapTFP {
    struct EdgeSwapMsg {
        edgeid_t edge_id;
        swapid_t swap_id;

        EdgeSwapMsg() { }
        EdgeSwapMsg(const edgeid_t &edge_id_, const swapid_t &swap_id_) : edge_id(edge_id_), swap_id(swap_id_) {}

        DECL_LEX_COMPARE_OS(EdgeSwapMsg, edge_id, swap_id);
    };

    struct DependencyChainEdgeMsg {
        swapid_t swap_id;
        // edgeid_t edge_id; is not used any more; we rather encode in the LSB of swap_id whether to target the first or second edge
        edge_t edge;

        DependencyChainEdgeMsg() { }

        DependencyChainEdgeMsg(const swapid_t &swap_id_, const edge_t &edge_)
              : swap_id(swap_id_), edge(edge_) { }

        DECL_LEX_COMPARE_OS(DependencyChainEdgeMsg, swap_id, edge);
    };

    struct DependencyChainSuccessorMsg {
        swapid_t swap_id;
        swapid_t successor;

        DependencyChainSuccessorMsg() { }

        DependencyChainSuccessorMsg(const swapid_t &swap_id_, const swapid_t &successor_) :
              swap_id(swap_id_), successor(successor_) { }

        DECL_LEX_COMPARE_OS(DependencyChainSuccessorMsg, swap_id, successor);
    };

    struct ExistenceRequestMsg {
        edge_t edge;
        swapid_t flagged_swap_id;

        swapid_t swap_id() const {return flagged_swap_id >> 1;}
        bool forward_only() const {return !(flagged_swap_id & 1);}

        ExistenceRequestMsg() { }

        ExistenceRequestMsg(const edge_t &edge_, const swapid_t &swap_id_, const bool &forward_only_) :
              edge(edge_), flagged_swap_id( (swap_id_<<1) | (!forward_only_)) { }

        bool operator< (const ExistenceRequestMsg& o) const {
            return (edge < o.edge || (edge == o.edge && (flagged_swap_id > o.flagged_swap_id)));
        }
        DECL_TO_TUPLE(edge, flagged_swap_id);
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

    class EdgeSwapTFP : public EdgeSwapBase {
    protected:
        constexpr static size_t _pq_mem = PQ_INT_MEM;
        constexpr static size_t _pq_pool_mem = PQ_POOL_MEM;
        constexpr static size_t _sorter_mem = SORTER_MEM;

        constexpr static bool compute_stats = false;
        constexpr static bool produce_debug_vector=false;
        constexpr static bool _async_processing = false;

// memory estimation
        class MemoryEstimation {
        public:
            size_t depchain_edge_sorter() const {return std::get<0>(_sizes[0]);}
            stxxl::unsigned_type depchain_pq_pool() const {return std::get<0>(_sizes[1]);}
            size_t depchain_successor_sorter() const {return std::get<0>(_sizes[2]);}

            stxxl::unsigned_type edge_state_pq_pool() const {return std::get<0>(_sizes[3]);}
            size_t edge_swap_sorter() const {return std::get<0>(_sizes[4]);}
            size_t edge_update_sorter() const {return std::get<0>(_sizes[5]);}

            stxxl::unsigned_type existence_info_pq_pool() const {return std::get<0>(_sizes[6]);}
            size_t existence_info_sorter() const {return std::get<0>(_sizes[7]);}
            size_t existence_request_sorter() const {return std::get<0>(_sizes[8]);}
            size_t existence_successor_sorter() const {return std::get<0>(_sizes[9]);}


            MemoryEstimation(const size_t& mem, const swapid_t& no_swaps, const degree_t avg_deg)
                    : _sizes( _compute(mem, no_swaps, avg_deg) )
            {}

        protected:
            using size_block_t = std::tuple<size_t, size_t, size_t>;
            using size_array_t = std::array<size_block_t, 10>;
            const size_array_t _sizes;
            size_array_t _compute(const size_t& mem, const swapid_t& no_swaps, const degree_t& avg_deg) const;
        };
        const MemoryEstimation _mem_est;

// graph
        using edge_buffer_t = EdgeStream;

        const swapid_t _run_length;
        edge_buffer_t &_edges;

        std::unique_ptr<std::thread> _result_thread;

// swap -> edge
        using EdgeSwapComparator = typename GenericComparatorStruct<EdgeSwapMsg>::Ascending;
        using EdgeSwapSorter = stxxl::sorter<EdgeSwapMsg, EdgeSwapComparator>;
        std::unique_ptr<EdgeSwapSorter> _edge_swap_sorter;
        BoolStream _swap_directions;

        swapid_t _next_swap_id_pushing;
        std::unique_ptr<EdgeSwapSorter> _edge_swap_sorter_pushing;
        BoolStream _swap_directions_pushing;

// dependency chain
        // we need to use a desc-comparator since the pq puts the largest element on top
        using DependencyChainEdgeComparatorSorter = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Ascending;
        using DependencyChainEdgeSorter = stxxl::sorter<DependencyChainEdgeMsg, DependencyChainEdgeComparatorSorter>;
        DependencyChainEdgeSorter _depchain_edge_sorter;

        using DependencyChainSuccessorComparator = typename GenericComparatorStruct<DependencyChainSuccessorMsg>::Ascending;
        using DependencyChainSuccessorSorter = stxxl::sorter<DependencyChainSuccessorMsg, DependencyChainSuccessorComparator>;
        DependencyChainSuccessorSorter _depchain_successor_sorter;

        std::unique_ptr<std::thread> _depchain_thread;

        using EdgeIdVector = stxxl::VECTOR_GENERATOR<edgeid_t>::result;

// existence requests
        using ExistenceRequestComparator = typename GenericComparatorStruct<ExistenceRequestMsg>::Ascending;
        using ExistenceRequestSorter = stxxl::sorter<ExistenceRequestMsg, ExistenceRequestComparator>;
        ExistenceRequestSorter _existence_request_sorter;

// existence information and dependencies
        using ExistenceInfoComparator = typename GenericComparatorStruct<ExistenceInfoMsg>::Ascending;
        using ExistenceInfoSorter = stxxl::sorter<ExistenceInfoMsg, ExistenceInfoComparator>;
        ExistenceInfoSorter _existence_info_sorter;

        using ExistenceSuccessorComparator = typename GenericComparatorStruct<ExistenceSuccessorMsg>::Ascending;
        using ExistenceSuccessorSorter = stxxl::sorter<ExistenceSuccessorMsg, ExistenceSuccessorComparator>;
        ExistenceSuccessorSorter _existence_successor_sorter;

// edge updates
        using EdgeUpdateComparator = typename GenericComparator<edge_t>::Ascending;
        using EdgeUpdateSorter = stxxl::sorter<edge_t, EdgeUpdateComparator>;
        EdgeUpdateSorter _edge_update_sorter;
        std::unique_ptr<std::thread> _edge_update_sorter_thread;

// PQ used internally in _simulate_swaps and _perform_swaps
        using DependencyChainEdgeComparatorPQ = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Descending;
        using DependencyChainEdgePQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<DependencyChainEdgeMsg, DependencyChainEdgeComparatorPQ, _pq_mem, 1 << 20>::result;
        using DependencyChainEdgePQBlock = typename DependencyChainEdgePQ::block_type;

        stxxl::read_write_pool<DependencyChainEdgePQBlock> _dependency_chain_pq_pool;
        DependencyChainEdgePQ _dependency_chain_pq;

// PQ used internally in _perform_swaps
        // we need to use a desc-comparator since the pq puts the largest element on top
        using ExistenceInfoPQComparator = typename GenericComparatorStruct<ExistenceInfoMsg>::Descending;
        using ExistenceInfoPQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<ExistenceInfoMsg, ExistenceInfoPQComparator, _pq_mem, 1 << 20>::result;
        using ExistenceInfoPQBlock = typename ExistenceInfoPQ::block_type;

        stxxl::read_write_pool<ExistenceInfoPQBlock> _existence_info_pq_pool;
        ExistenceInfoPQ _existence_info_pq;

        BoolStream _edge_update_mask;
        BoolStream _last_edge_update_mask;

// algos
        void _gather_edges();

        template <class EdgeReader>
        void _compute_dependency_chain(EdgeReader&, BoolStream&);

        void _simulate_swaps();
        void _load_existence();
        void _perform_swaps();
        void _apply_updates();

        void _reset() {
            _edge_swap_sorter->clear();
            _depchain_edge_sorter.clear();
            _depchain_successor_sorter.clear();
            _existence_request_sorter.clear();
            _existence_info_sorter.clear();
            _existence_successor_sorter.clear();
        }

        bool _first_run;
        virtual void _process_swaps();

        virtual void _start_processing(bool async = true);
        std::thread _process_thread;

        bool _snapshots;
        int _frequency;
        int_t _itcount;
        int_t _hitcount;

    public:
        EdgeSwapTFP() = delete;
        EdgeSwapTFP(const EdgeSwapTFP &) = delete;

        //! Swaps are performed during constructor.
        //! @param edges  Edge vector changed in-place
        //! @param swaps  Read-only swap vector
        EdgeSwapTFP(edge_buffer_t &edges, const swapid_t& run_length, const node_t& num_nodes, const size_t& im_memory,
                    const bool snapshots = false, const int frequency = 0) :
              EdgeSwapBase(),
              _mem_est(im_memory, run_length, edges.size() / num_nodes),

              _run_length(run_length),
              _edges(edges),

              _edge_swap_sorter(new EdgeSwapSorter(EdgeSwapComparator(), _mem_est.edge_swap_sorter())),
              _next_swap_id_pushing(0),
              _edge_swap_sorter_pushing(new EdgeSwapSorter(EdgeSwapComparator(), _mem_est.edge_swap_sorter())),

              _depchain_edge_sorter(DependencyChainEdgeComparatorSorter{}, _mem_est.depchain_edge_sorter()),
              _depchain_successor_sorter(DependencyChainSuccessorComparator{}, _mem_est.depchain_successor_sorter()),
              _existence_request_sorter(ExistenceRequestComparator{}, _mem_est.existence_request_sorter()),
              _existence_info_sorter(ExistenceInfoComparator{}, _mem_est.existence_info_sorter()),
              _existence_successor_sorter(ExistenceSuccessorComparator{}, _mem_est.existence_successor_sorter()),
              _edge_update_sorter(EdgeUpdateComparator{}, _mem_est.edge_update_sorter()),

              _dependency_chain_pq_pool(_mem_est.depchain_pq_pool() / DependencyChainEdgePQBlock::raw_size,
                                        _mem_est.depchain_pq_pool() / DependencyChainEdgePQBlock::raw_size),
              _dependency_chain_pq(_dependency_chain_pq_pool),

              _existence_info_pq_pool(_mem_est.existence_info_pq_pool() / ExistenceInfoPQBlock::raw_size,
                                      _mem_est.existence_info_pq_pool() / ExistenceInfoPQBlock::raw_size),
              _existence_info_pq(_existence_info_pq_pool),

              _first_run(true),

              _snapshots(snapshots),
              _frequency(frequency),
              _itcount(0),
              _hitcount(0)
        { }

        EdgeSwapTFP(edge_buffer_t &edges, swap_vector &swaps, swapid_t run_length = 1000000) :
            EdgeSwapTFP(edges, run_length, edges.size(), 1llu << 30)
        {
            std::cerr << "Using deprecated EdgeSwapTFP constructor. This is likely much slower!" << std::endl;
            stxxl::STXXL_UNUSED(swaps);
        }

        void push(const SwapDescriptor & swap) {
           // Every swap k to edges i, j sends one message (edge-id, swap-id) to each edge.
           // We then sort the messages lexicographically to gather all requests to an edge
           // at the same place.

           _edge_swap_sorter_pushing->push(EdgeSwapMsg(swap.edges()[0], _next_swap_id_pushing++));
           _edge_swap_sorter_pushing->push(EdgeSwapMsg(swap.edges()[1], _next_swap_id_pushing++));
           _swap_directions_pushing.push(swap.direction());

           if (UNLIKELY(_next_swap_id_pushing > 2*_run_length)) {
               _start_processing();
               if (_snapshots)
                   if (++_itcount % _frequency == 0) {
                       std::ostringstream filename;
                       filename << "graph_snapshot_" << ++_hitcount << ".metis";
                       std::cout << "Exporting Snapshot " << _hitcount << std::endl;
                       export_as_metis_nonpointer(_edges, filename.str());
                       _edges.consume();
                   }
           }
        }



        void run();

    };
};

template <>
struct EdgeSwapTrait<EdgeSwapTFP::EdgeSwapTFP> {
    static bool swapVector() {return false;}
    static bool pushableSwaps() {return true;}
    static bool pushableSwapBuffers() {return false;}
    static bool edgeStream() {return true;}
};
