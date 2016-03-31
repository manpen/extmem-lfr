#pragma once

#include <cassert>
#include <stxxl/priority_queue>
#include <defs.h>
#include <vector>
#include <memory>
#include <PQSorterMerger.h>

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
            assert(_global_data._pq_output_buffer[target_tid * _global_data._num_threads + _tid]);
            assert(target_tid < _global_data._num_threads);
            _global_data._pq_output_buffer[target_tid * _global_data._num_threads + _tid]->push_back(std::forward<value_type>(v));
        };

        void push(const value_type& v, int target_tid) {
            assert(_global_data._pq_output_buffer[target_tid * _global_data._num_threads + _tid]);
            assert(target_tid < _global_data._num_threads);
            _global_data._pq_output_buffer[target_tid * _global_data._num_threads + _tid]->push_back(v);
        };

        /**
            * Flush the buffers of the current thread. Should be called for all thread-local data objects at the same time
            *
            * @warning While flush_buffer is called for a thread, no thread may push any value for that thread.
            */
        void flush_buffer() {
            for (int i = _tid * _global_data._num_threads; i < (_tid + 1) * _global_data._num_threads; ++i) {
                assert(_global_data._pq_output_buffer[i]);
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