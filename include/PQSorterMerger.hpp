#pragma once

#include <iostream>

/***
 * Merge a STXXL Sorter and Priority Queue
 *
 * In case a large portion of data in a PQ is computed prior to reading access,
 * or if there are multiple passes over a static data sequence, it can be beneficial
 * to use a sorter for this offline data.
 *
 * The value type and comparator are derived from the PQ.
 */
template <class PQ, class Sorter, bool compute_stats = false>
class PQSorterMerger {
public:
    using value_type = typename PQ::value_type;

private:
    using Comp = typename PQ::comparator_type;
    PQ& _pq;
    Sorter& _sorter;
    Comp _comp;

    enum source_type {SrcPQ, SrcSort};
    source_type _value_src;
    value_type  _value;

    uint64_t _elements_from_pq;
    uint64_t _elements_from_sorter;

    void _fetch() {
        assert(!empty());

        // in case one source is empty, we cannot safely use the comparator
        if (UNLIKELY(_pq.empty())) {
            _value = *_sorter;
            _value_src = SrcSort;
        } else if (UNLIKELY(_sorter.empty())) {
            _value = _pq.top();
            _value_src = SrcPQ;
        } else if (_comp(_pq.top(), *_sorter)) {
            _value = *_sorter;
            _value_src = SrcSort;
        } else {
            _value = _pq.top();
            _value_src = SrcPQ;
        }
    }

public:
    PQSorterMerger() = delete;

    PQSorterMerger(PQ & pq, Sorter & sorter) :
          _pq(pq), _sorter(sorter), _elements_from_pq(0), _elements_from_sorter(0)
    {
        if (!empty())
            _fetch();
    }

    //! Call in case the PQ/Sorter are changed externally
    void update() {
        if (LIKELY(!empty()))
            _fetch();
    }

    //! Push an item into the PQ and update the merger
    void push(const value_type& o) {
        _pq.push();
        _fetch();
    }

    //! Returns true if PQ and Sorter are empty
    bool empty() const {
        return _pq.empty() && _sorter.empty();
    }

    //! Removes the smallest element from its source and fetches
    //! next (if availble)
    //! @note Call only if sorter is in output mode and empty() == false
    PQSorterMerger& operator++() {
        assert(!empty());
        if (_value_src == SrcPQ) {
            _pq.pop();
            if (compute_stats) _elements_from_pq++;
        } else {
            ++_sorter;
            if (compute_stats) _elements_from_sorter++;
        }

        if (LIKELY(!empty()))
            _fetch();

        return *this;
    }

    //! Returns reference to the smallest item
    //! @note Access only if not empty
    const value_type & operator*() const {
        assert(!empty());
        return _value;
    }

    //! If compute_stats=true output statistics to STDOUT
    void dump_stats() const {
        if (!compute_stats)
            return;

        auto elements_tot = _elements_from_pq + _elements_from_sorter;
        std::cout << "Elements consumed: " << elements_tot
                  << " from PQ: " << (_elements_from_pq) << " (" << (100.0 * _elements_from_pq / elements_tot) << "%)"
                     " from Sorter: " << (_elements_from_sorter) << " (" << (100.0 * _elements_from_sorter / elements_tot) << "%)\n";
    }
};