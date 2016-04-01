#pragma once

#include <cassert>
#include <stxxl/priority_queue>
#include <defs.h>
#include <vector>
#include <memory>
#include <PQSorterMerger.h>

template <typename sorter_t, typename pq_comparator_t>
class ParallelBufferedPQSorterMerger {
public:
    using value_type = typename sorter_t::value_type;
    using PQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<value_type, pq_comparator_t, PQ_INT_MEM, 1 << 20>::result;
    using PQBlock = typename PQ::block_type;
private:
    const int _num_threads;

    std::vector<std::unique_ptr<std::vector<value_type>>> _pq_output_buffer;

    class stream {
    private:
        const int _tid;
        PQSorterMerger<PQ, sorter_t>& _pqsorter;
    public:
        stream(PQSorterMerger<PQ, sorter_t>& pqsorter, const int tid) : _tid(tid), _pqsorter(pqsorter) {
        }

        // stream interface
        bool empty() const  {
            return _pqsorter.empty();
        };

        stream& operator++() {
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

    class ThreadData {
    private:
        ParallelBufferedPQSorterMerger& _global_data;
        const int _tid;
        stxxl::read_write_pool<PQBlock> _pq_pool;
        PQ _pq;
        sorter_t _input_sorter;
        PQSorterMerger<PQ, sorter_t> _pqsorter;
    public:

        ThreadData(ParallelBufferedPQSorterMerger& global_data, int tid) :
            _global_data(global_data),
            _tid(tid),
            _pq_pool(PQ_POOL_MEM / 2 / PQBlock::raw_size, PQ_POOL_MEM / 2 / PQBlock::raw_size),
            _pq(_pq_pool),
            _input_sorter(typename sorter_t::cmp_type(), SORTER_MEM),
            _pqsorter(_pq, _input_sorter, false)
            {
                for (int i = 0; i < _global_data._num_threads; ++i) {
                    _global_data._pq_output_buffer[i * _global_data._num_threads + _tid].reset(new std::vector<value_type>);
                }
            };

        void push_sorter(value_type&&v) {
            _input_sorter.push(std::forward<value_type>(v));
        };

        void finish_sorter_input() {
            _input_sorter.sort();
            _pqsorter.update();
        };

        void rewind_sorter() {
            assert(_pq.empty());
            _input_sorter.rewind();
            _pqsorter.update();
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

        void clear() {
            _input_sorter.clear();
            while (!_pq.empty()) {
                _pq.pop();
            }
        };

        stream get_stream() {
            return stream(_pqsorter, _tid);
        };

    };

    std::vector<std::unique_ptr<ThreadData>> _thread_data;
public:

    ParallelBufferedPQSorterMerger(int num_threads) :
        _num_threads(num_threads),
        _pq_output_buffer(_num_threads * _num_threads),
        _thread_data(num_threads) { }

    void initialize(int tid) {
        assert(tid < _num_threads);
        if (!_thread_data[tid]) {
            _thread_data[tid].reset(new ThreadData(*this, tid));
        }
    }

    void push_sorter(value_type&& v, int target_tid) {
        assert(_thread_data[target_tid]);
        _thread_data[target_tid]->push_sorter(std::forward<value_type>(v));
    }

    void finish_sorter_input(int tid)  {
        assert(_thread_data[tid]);
        _thread_data[tid]->finish_sorter_input();
    };

    void rewind_sorter(int tid) {
        assert(_thread_data[tid]);
        _thread_data[tid]->rewind_sorter();
    }

    void clear(int tid) {
        assert(_thread_data[tid]);
        _thread_data[tid]->clear();
    }

    void push_pq(int tid, value_type&& v, int target_tid) {
        assert(_thread_data[tid]);
        assert(_thread_data[target_tid]);
        _thread_data[tid]->push(std::forward<value_type>(v), target_tid);
    }

    void flush_pq_buffer(int tid) {
        assert(_thread_data[tid]);
        _thread_data[tid]->flush_buffer();
    }

    stream get_stream(int tid) {
        assert(_thread_data[tid]);
        return _thread_data[tid]->get_stream();
    }

};