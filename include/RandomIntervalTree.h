#pragma once
#include <vector>
#include <stxxl/bits/common/utils.h>


template <typename T = uint64_t>
class RandomIntervalTree {
public:
    using value_type = T;
    using Index = uint64_t;

protected:    
    const unsigned int _layers;
    const Index _inner_nodes_offset;

    std::vector<T> _tree;
    T* const _tree_data;
    T _total_weight;

    template <typename T1>
    void _build(const std::vector<T1>& leaves) {
        const Index n = leaves.size();

        // FIX-ME: we can to it in linear time
        for(Index i=0; i < n; i++) {
            auto idx = i+_inner_nodes_offset;
            _total_weight += leaves[i];

            for(unsigned int l=_layers+1; l; --l) {
                auto parent = idx >> 1;
                bool is_right_child = idx & 1;

                _tree_data[parent] += leaves[i] * !is_right_child;

                idx = parent;
            }

            assert(!idx);
        }
    }

public:
    template <typename T1>
    RandomIntervalTree(const std::vector<T1>& leaves)
        : _layers(stxxl::ilog2_ceil(leaves.size()))
        , _inner_nodes_offset((Index(1) << _layers))
        , _tree(_inner_nodes_offset, 0)
        , _tree_data(_tree.data() - 1)
        , _total_weight(0)
    {
        _build(leaves);
    }

    Index getLeaf(T weight) const {
        Index idx = 1;

        assert(weight <= _total_weight);

        for(unsigned int l=_layers; l; --l) {
            assert(idx <= _tree.size());
            bool to_right = (weight >= _tree_data[idx]);
            weight -= _tree_data[idx] * to_right;
            idx = 2*idx + to_right;
        }

        assert(idx >=  _inner_nodes_offset);
        assert(idx  <2*_inner_nodes_offset);

        return idx - _inner_nodes_offset;
    }

    void decreaseLeaf(Index leaf_idx) {
        assert(leaf_idx <= _tree.size());
        leaf_idx += _inner_nodes_offset;

        _total_weight--;
        for(unsigned int l=_layers; l; --l) {
            auto is_right_child = leaf_idx & 1;
            leaf_idx /= 2;

            assert(is_right_child || _tree_data[leaf_idx]);
            _tree_data[leaf_idx] -= !is_right_child;
        }

        assert(leaf_idx == 1);
    }

    void dump() {
        Index k=0;
        for(unsigned int l=0; l<_layers; l++) {
            for(Index i=0; i < (Index(1)<<l); i++) {
                std::cout << (k < _tree.size() ? std::to_string(_tree[k]) : "--") << " ";
                ++k;
            }

            std::cout << std::endl;
        }
    }

    value_type total_weight() const {
        return _total_weight;
    }
};
