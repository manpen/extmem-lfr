#pragma once

#include <cassert>
#include <stxxl/bits/containers/parallel_priority_queue.h>
#include <defs.h>
#include <vector>
#include <memory>
#include <PQSorterMerger.h>

template <typename sorter_t, typename pq_comparator_t>
class ParallelBufferedPQSorterMerger {
public:
    using value_type = typename sorter_t::value_type;
    using PQ = stxxl::parallel_priority_queue<value_type, pq_comparator_t>;
private:
    const int _num_threads;
    PQ _pq;
    sorter_t _sorter;

    value_type _cur;
    value_type _limit;
    bool _empty;

    void fetch_next() {
        if (LIKELY(!_sorter.empty()) && (_pq.empty() || *_sorter < _pq.top())) {
            _cur = *_sorter;
            _empty = (_cur >= _limit);

            if (LIKELY(!_empty)) {
                ++_sorter;
            }
        } else if (LIKELY(!_pq.empty())) {
            _cur = _pq.top();
            _empty = (_cur >= _limit);
            if (LIKELY(!_empty)) {
                _pq.pop();
            }
        } else {
            _empty = true;
        }
    };
public:
    ParallelBufferedPQSorterMerger(int num_threads) :
        _num_threads(num_threads),
        _pq(pq_comparator_t(), PQ_INT_MEM, 1.5f, 14, num_threads),
        _sorter(typename sorter_t::cmp_type(), SORTER_MEM)
        { }

    void push_sorter(value_type&& v) {
        _sorter.push(v);
    }

    void finish_sorter_input()  {
        _sorter.sort();
    };

    void rewind_sorter() {
        _sorter.rewind();
    }

    void clear() {
        _sorter.clear();
    }

    void start_batch(const value_type& limit) {
        _limit = limit;
        fetch_next();
    };

    void start_push() {
        _pq.limit_begin(_limit, 100);
    };

    bool empty() {
        return _empty;
    };

    const value_type& operator*() {
        return _cur;
    };

    const value_type* operator->() {
        return &_cur;
    };

    ParallelBufferedPQSorterMerger& operator++() {
        fetch_next();
        return *this;
    };

    void push_pq(int tid, const value_type& v) {
        assert(v > _limit);
        _pq.limit_push(v, tid);
    }

    void end_batch() {
        _pq.limit_end();
    }
};