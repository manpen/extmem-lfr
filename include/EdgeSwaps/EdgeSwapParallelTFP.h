#pragma once

#include <stxxl/vector>
#include <stxxl/sorter>
#include <stxxl/bits/unused.h>
#include <stxxl/priority_queue>

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
#include <PQSorterMerger.h>

namespace EdgeSwapParallelTFP {
// TODO: get rid of spos. Use two swap ids per swap instead.
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

    template <typename sorter_t, typename pq_comparator_t>
    class ParallelBufferedPQSorterMerger {
    private:
        using value_type = typename sorter_t::value_type;
        using PQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<value_type, pq_comparator_t, PQ_INT_MEM, 1 << 20>::result;
        using PQBlock = typename PQ::block_type;
        const int _num_threads;

        std::vector<std::unique_ptr<sorter_t>>& _input_sorters;
        std::vector<std::unique_ptr<std::vector<value_type>>> _pq_output_buffer;
    public:
        ParallelBufferedPQSorterMerger(int num_threads, std::vector<std::unique_ptr<sorter_t>>& input_sorters) :
            _num_threads(num_threads),
            _input_sorters(input_sorters),
            _pq_output_buffer(_num_threads * _num_threads) {
                assert(_input_sorters.size() == static_cast<size_t>(_num_threads));
            }

        class ThreadData {
        private:
            ParallelBufferedPQSorterMerger& _global_data;
            const int _tid;
            stxxl::read_write_pool<PQBlock> _pq_pool;
            PQ _pq;
            PQSorterMerger<PQ, sorter_t> _pqsorter;
        public:
            ThreadData(ParallelBufferedPQSorterMerger& global_data, int tid) :
                _global_data(global_data),
                _tid(tid),
                _pq_pool(PQ_POOL_MEM / 2 / PQBlock::raw_size, PQ_POOL_MEM / 2 / PQBlock::raw_size),
                _pq(_pq_pool),
                _pqsorter(_pq, *_global_data._input_sorters[tid])
                {
                    for (int i = 0; i < _global_data._num_threads; ++i) {
                        _global_data._pq_output_buffer[i * _global_data._num_threads + _tid].reset(new std::vector<value_type>);
                    }
                };

            void push(value_type&& v, int target_tid) {
                _global_data._pq_output_buffer[target_tid * _global_data._num_threads + _tid]->push_back(std::forward<value_type>(v));
            };

            void push(const value_type& v, int target_tid) {
                _global_data._pq_output_buffer[target_tid * _global_data._num_threads + _tid]->push_back(v);
            };

            /**
             * Flush the buffers of the current thread. Should be called for all thread-local data objects at the same time
             *
             * @warning While flush_buffer is called for a thread, no thread may push any value for that thread.
             */
            void flush_buffer() {
                for (int i = _tid * _global_data._num_threads; i < (_tid + 1) * _global_data._num_threads; ++i) {
                    for (const auto & v : *_global_data._pq_output_buffer[i]) {
                        _pq.push(v);
                    }
                    _global_data._pq_output_buffer[i]->clear();
                }
                _pqsorter.update();
            };

            // stream interface
            bool empty() const  {
                return _pqsorter.empty();
            };

            ThreadData& operator++() {
                ++_pqsorter;
                return *this;
            };

            const value_type & operator*() const {
                return *_pqsorter;
            };

            const value_type * operator->() const {
                return &(operator * ());
            };
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
        swapid_t _swap_id;

#ifdef EDGE_SWAP_DEBUG_VECTOR
        debug_vector::bufwriter_type _debug_vector_writer;
#endif

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
        void _load_and_update_edges(std::vector<std::unique_ptr<DependencyChainEdgeSorter>>& edge_output, std::vector<std::unique_ptr<DependencyChainSuccessorSorter>>& dependency_output);
        void _compute_conflicts(std::vector<std::unique_ptr<DependencyChainEdgeSorter>>& edges, std::vector<std::unique_ptr<DependencyChainSuccessorSorter>>& dependencies, ExistenceRequestMerger& requestOutputMerger);
        void _process_existence_requests(ExistenceRequestMerger& requestMerger,
            std::vector<std::unique_ptr<ExistenceInfoSorter>>& existence_info_output, std::vector<std::unique_ptr<ExistenceSuccessorSorter>>& successor_output,
            std::vector<std::unique_ptr<ExistencePlaceholderSorter>>& existence_placeholder_output);
        void _perform_swaps(std::vector<std::unique_ptr<DependencyChainEdgeSorter>>& edges, std::vector<std::unique_ptr<DependencyChainSuccessorSorter>>& edge_dependencies,
            std::vector<std::unique_ptr<ExistenceInfoSorter>>& existence_info, std::vector<std::unique_ptr<ExistenceSuccessorSorter>>& existence_successor,
            std::vector<std::unique_ptr<ExistencePlaceholderSorter>>& existence_placeholder);

    public:
        EdgeSwapParallelTFP() = delete;
        EdgeSwapParallelTFP(const EdgeSwapParallelTFP &) = delete;

        //! Swaps are performed during constructor.
        //! @param edges  Edge vector changed in-place
        //! @param swaps  Read-only swap vector - ignored!
        EdgeSwapParallelTFP(edge_vector &edges, swap_vector &, swapid_t swaps_per_iteration = 1000000) :
              EdgeSwapParallelTFP(edges, swaps_per_iteration) { }

        EdgeSwapParallelTFP(edge_vector &edges, swapid_t swaps_per_iteration, int num_threads = omp_get_max_threads()) :
              EdgeSwapBase(),
              _edges(edges),
              _num_swaps_per_iteration(swaps_per_iteration),
              _swap_id(0),
#ifdef EDGE_SWAP_DEBUG_VECTOR
              _debug_vector_writer(_result),
#endif

              _swap_direction(num_threads),
              _swap_direction_writer(num_threads),
              _edge_swap_sorter(GenericComparatorStruct<EdgeLoadRequest>::Ascending(), _sorter_mem),
              _edge_update_merger(EdgeUpdateComparator{}, _sorter_mem),
              _num_threads(num_threads) {
                for (int i = 0; i < _num_threads; ++i) {
                    _swap_direction[i].reset(new BoolVector);
                    _swap_direction[i]->resize(swaps_per_iteration);
                    _swap_direction_writer[i].reset(new BoolVector::bufwriter_type(*_swap_direction[i]));
                }
              } // FIXME actually _edge_update_sorter isn't needed all the time. If memory is an issue, we could safe memory here

        void process_swaps();
        void run() {
            process_swaps();
#ifdef EDGE_SWAP_DEBUG_VECTOR
            _debug_vector_writer.finish();
#endif
        };

        void push(const swap_descriptor& swap) {
            _edge_swap_sorter.push(EdgeLoadRequest {swap.edges()[0], _swap_id, 0});
            _edge_swap_sorter.push(EdgeLoadRequest {swap.edges()[1], _swap_id, 1});
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