#pragma once

#include <stxxl/vector>
#include <stxxl/sorter>
#include <stxxl/bits/unused.h>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <utility>
#include <stack>

#include <defs.h>
#include "Swaps.h"
#include "GenericComparator.h"
#include "TupleHelper.h"

#include "EdgeSwapBase.h"
#include "BoolStream.h"
#include <omp.h>
#include <ParallelBufferedPQSorterMerger.h>

namespace EdgeSwapParallelTFP {
    inline swapid_t get_swap_id(swapid_t sid) {
        return sid/2;
    };

    inline unsigned char get_swap_spos(swapid_t sid) {
        return sid%2;
    };

    inline swapid_t pack_swap_id_spos(swapid_t swap_id, unsigned char spos) {
        return swap_id * 2 + spos;
    };

    struct EdgeLoadRequest {
        edgeid_t eid;
        swapid_t sid;

        DECL_LEX_COMPARE_OS(EdgeLoadRequest, eid, sid);
    };

    struct DependencyChainEdgeMsg {
        swapid_t sid;
        edge_t edge;

        DECL_LEX_COMPARE_OS(DependencyChainEdgeMsg, sid, edge);
    };

    struct DependencyChainSuccessorMsg {
        swapid_t sid;
        swapid_t successor;

        DECL_LEX_COMPARE_OS(DependencyChainSuccessorMsg, sid); // NOTE other fields are not necessary for uniqueness and sorting.
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

        ExistenceInfoMsg() { }

        ExistenceInfoMsg(const swapid_t &swap_id_, const edge_t &edge_) :
            swap_id(swap_id_), edge(edge_)
        { }

        DECL_LEX_COMPARE_OS(ExistenceInfoMsg, swap_id, edge);
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


    template <class runs_creator_t>
    class RunsCreatorThread {
    private:
        using buffer_type = std::vector<typename runs_creator_t::value_type>;

        struct Task {
            buffer_type requests;
            std::promise<buffer_type> promise;
            runs_creator_t &runs_creator;

            Task(buffer_type && requests, runs_creator_t &runs_creator) : requests(requests), runs_creator(runs_creator) {};

            Task(Task&&) = default;

            void operator()() {
                for (auto & req : requests) {
                    runs_creator.push(req);
                }
                runs_creator.finish();

                promise.set_value(std::move(requests));
            };

            std::future<buffer_type> get_future() {
                return promise.get_future();
            };
        };

        runs_creator_t& runs_creator;
        bool is_running;
        std::stack<Task> tasks;
        std::mutex mutex;
        std::condition_variable items_available;
        std::thread worker_thread;

    public:
        RunsCreatorThread(runs_creator_t& runs_creator) :
            runs_creator(runs_creator),
            is_running(true),
            worker_thread([this]() {
                std::unique_lock<std::mutex> lk(this->mutex);
                while (this->is_running) {
                    while (!this->tasks.empty()) {
                        auto t = std::move(this->tasks.top());
                        this->tasks.pop();

                        lk.unlock();

                        t();

                        lk.lock();
                    }

                    this->items_available.wait(lk, [&](){return !this->is_running || !this->tasks.empty();});
                }
            })
        {};

        ~RunsCreatorThread() {
            {
                std::lock_guard<std::mutex> lk(mutex);
                is_running = false;
            }

            items_available.notify_all();

            worker_thread.join();
        };

        std::future<buffer_type> enqueue_task(buffer_type &&existence_requests) {
            std::future<buffer_type> result;
            {
                std::lock_guard<std::mutex> lk(mutex);
                tasks.emplace(std::forward<buffer_type>(existence_requests), runs_creator);
                result = tasks.top().get_future();
            }
            items_available.notify_one();

            return result;
        };
    };

    template <class runs_creator_t>
    class RunsCreatorBuffer {
    private:
        using buffer_type = std::vector<typename runs_creator_t::value_type>;
        RunsCreatorThread<runs_creator_t>& _runs_creator_thread;
        size_t _expected_buffer_size;
        buffer_type _buffer;
        std::future<buffer_type> _future;
    public:
        RunsCreatorBuffer(RunsCreatorThread<runs_creator_t>& runs_creator_thread, size_t expected_buffer_size) :
            _runs_creator_thread(runs_creator_thread), _expected_buffer_size(expected_buffer_size) {
            _buffer.reserve(expected_buffer_size);
        };

        ~RunsCreatorBuffer() {
            flush();
        };

        void push(typename runs_creator_t::value_type&& v) {
            _buffer.push_back(std::forward<typename runs_creator_t::value_type>(v));
        };

        void push(const typename runs_creator_t::value_type& v) {
            _buffer.push_back(v);
        };

        void finish() {
            std::sort(_buffer.begin(), _buffer.end());

            // make sure the old buffer has been written
            flush();

            auto f = _runs_creator_thread.enqueue_task(std::move(_buffer));
            if (_future.valid()) {
                _buffer = _future.get();
            }

            // this should not cause any re-allocations unless the future was not valid
            _buffer.clear();
            _buffer.reserve(_expected_buffer_size);

            _future = std::move(f);
        };

        void flush() {
            if (_future.valid()) {
                _future.wait();
            }
        };
    };


    class EdgeSwapParallelTFP : public EdgeSwapBase {
    protected:
        constexpr static size_t _pq_mem = PQ_INT_MEM;
        constexpr static size_t _pq_pool_mem = PQ_POOL_MEM;
        constexpr static size_t _sorter_mem = SORTER_MEM;

        constexpr static bool compute_stats = false;
        constexpr static bool produce_debug_vector=true;

        edge_vector &_edges;
        swapid_t _num_swaps_per_iteration;
        swapid_t _num_swaps_in_run;

#ifdef EDGE_SWAP_DEBUG_VECTOR
        debug_vector::bufwriter_type _debug_vector_writer;
#endif

// swap direction information
        std::vector<std::unique_ptr<BoolStream>> _swap_direction;

// swap -> edge
        using EdgeLoadRequestSorter = stxxl::sorter<EdgeLoadRequest, GenericComparatorStruct<EdgeLoadRequest>::Ascending>;
        EdgeLoadRequestSorter _edge_swap_sorter;

// dependency chain
        // we need to use a desc-comparator since the pq puts the largest element on top
        using DependencyChainEdgeComparatorSorter = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Ascending;
        using DependencyChainEdgeComparatorPQ = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Descending;
        using DependencyChainEdgeSorter = stxxl::sorter<DependencyChainEdgeMsg, DependencyChainEdgeComparatorSorter>;
        ParallelBufferedPQSorterMerger<DependencyChainEdgeSorter, DependencyChainEdgeComparatorPQ> _edge_state;

        using DependencyChainSuccessorComparator = typename GenericComparatorStruct<DependencyChainSuccessorMsg>::Ascending;
        using DependencyChainSuccessorSorter = stxxl::sorter<DependencyChainSuccessorMsg, DependencyChainSuccessorComparator>;

        BoolStream _valid_edges;
        bool _needs_writeback;

// existence requests
        using ExistenceRequestComparator = typename GenericComparatorStruct<ExistenceRequestMsg>::Ascending;
        using ExistenceRequestSorter = stxxl::sorter<ExistenceRequestMsg, ExistenceRequestComparator>;
        using ExistenceRequestMerger = ExistenceRequestSorter::runs_merger_type;

// existence information and dependencies
        using ExistenceInfoComparator = typename GenericComparatorStruct<ExistenceInfoMsg>::Ascending;
        using ExistenceInfoComparatorPQ = typename GenericComparatorStruct<ExistenceInfoMsg>::Descending;
        using ExistenceInfoSorter = stxxl::sorter<ExistenceInfoMsg, ExistenceInfoComparator>;
        ParallelBufferedPQSorterMerger<ExistenceInfoSorter, ExistenceInfoComparatorPQ> _existence_info;

        using ExistenceSuccessorComparator = typename GenericComparatorStruct<ExistenceSuccessorMsg>::Ascending;
        using ExistenceSuccessorSorter = stxxl::sorter<ExistenceSuccessorMsg, ExistenceSuccessorComparator>;

        using ExistencePlaceholderComparator = typename GenericComparator<swapid_t>::Ascending;
        using ExistencePlaceholderSorter = stxxl::sorter<swapid_t, ExistencePlaceholderComparator>;

// edge updates
        using EdgeUpdateComparator = typename GenericComparator<edge_t>::Ascending;
        using EdgeUpdateSorter = stxxl::sorter<edge_t, EdgeUpdateComparator>;
        using EdgeUpdateMerger = EdgeUpdateSorter::runs_merger_type;
        EdgeUpdateMerger _edge_update_merger;

// threads
        int _num_threads;

        int _thread(swapid_t swap_id) {
            return swap_id % _num_threads;
        };

// algos
        void _load_and_update_edges(std::vector<std::unique_ptr<DependencyChainSuccessorSorter>>& dependency_output);
        void _compute_conflicts(std::vector<std::unique_ptr<DependencyChainSuccessorSorter>>& dependencies, ExistenceRequestMerger& requestOutputMerger);
        void _process_existence_requests(ExistenceRequestMerger& requestMerger,
            std::vector<std::unique_ptr<ExistenceSuccessorSorter>>& successor_output,
            std::vector<std::unique_ptr<ExistencePlaceholderSorter>>& existence_placeholder_output);
        void _perform_swaps(std::vector<std::unique_ptr<DependencyChainSuccessorSorter>>& edge_dependencies,
            std::vector<std::unique_ptr<ExistenceSuccessorSorter>>& existence_successor,
            std::vector<std::unique_ptr<ExistencePlaceholderSorter>>& existence_placeholder);

    public:
        EdgeSwapParallelTFP() = delete;
        EdgeSwapParallelTFP(const EdgeSwapParallelTFP &) = delete;

        //! Swaps are performed during constructor.
        //! @param edges  Edge vector changed in-place
        //! @param swaps  Read-only swap vector - ignored!
        EdgeSwapParallelTFP(edge_vector &edges, swap_vector &, swapid_t swaps_per_iteration = 10000000);

        EdgeSwapParallelTFP(edge_vector &edges, swapid_t swaps_per_iteration, int num_threads = omp_get_max_threads());

        void process_swaps();
        void run();

        void push(const swap_descriptor& swap) {
            _swap_direction[_thread(_num_swaps_in_run)]->push(swap.direction());
            _edge_swap_sorter.push(EdgeLoadRequest {swap.edges()[0], pack_swap_id_spos(_num_swaps_in_run, 0)});
            _edge_swap_sorter.push(EdgeLoadRequest {swap.edges()[1], pack_swap_id_spos(_num_swaps_in_run, 1)});
            ++_num_swaps_in_run;
            if (_num_swaps_in_run >= _num_swaps_per_iteration) {
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