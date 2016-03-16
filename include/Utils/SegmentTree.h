/**
 * @file
 * @brief Test cases for TupleHelpers
 * @author Manuel Penschuck
 * @copyright
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * @copyright
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#ifndef NDEBUG
    #include <sstream>
    #include <stack>
#endif

#include <stxxl/bits/common/types.h>
#include <stxxl/bits/common/uint_types.h>
#include <stxxl/bits/common/utils.h>

// This macro results in a memory usage more than twice as big, but eases debugging,
// since it add every token _verbose fields which can easily read during debugging.
//#define SEGMENT_TREE_VERBOSE_TOKENS

namespace SegmentTree {
    enum TokenType : bool {
        QuerySegment = 0,
        NewSegment = 1
    };

    template <typename WeightT = stxxl::uint40, typename IdT = stxxl::uint48>
    class Token {
        constexpr static stxxl::uint64 WEIGHT_MASK = (1llu << (8*sizeof(WeightT))) - 1;
        constexpr static stxxl::uint64 TYPE_MASK = (1llu << (8*sizeof(IdT)-1));
        constexpr static stxxl::uint64 ID_MASK = ~TYPE_MASK;

        WeightT _weight;
        IdT _id;

#ifdef SEGMENT_TREE_VERBOSE_TOKENS
        TokenType _verbose_type;
        uint64_t _verbose_weight;
        uint64_t _verbose_id;
#endif

    public:
        Token() {}

        Token(TokenType type, uint64_t weight, uint64_t id)
            : _weight(static_cast<stxxl::uint64>(weight))
            , _id(static_cast<stxxl::uint64>((id & ID_MASK) | (TYPE_MASK * type)))
        {
#ifdef SEGMENT_TREE_VERBOSE_TOKENS
            _verbose_type = this->type();
            _verbose_weight = this->weight();
            _verbose_id = this->id();
#endif

            assert(weight == (weight & WEIGHT_MASK));
            assert(id     == (id     & ID_MASK));
        }

        TokenType type() const {
            return static_cast<TokenType>(static_cast<bool>(static_cast<stxxl::uint64>(_id) & TYPE_MASK));
        }

        uint64_t weight() const {
            return static_cast<stxxl::uint64>(_weight);
        }

        uint64_t id() const {
            return static_cast<stxxl::uint64>(_id) & ID_MASK;
        }

        void set_weight(uint64_t v) {
            assert(v == (v & WEIGHT_MASK));
            _weight = WeightT(static_cast<stxxl::uint64>(v));
#ifdef SEGMENT_TREE_VERBOSE_TOKENS
            _verbose_weight = weight();
#endif
        }
    };

    enum TreeNodeType : bool {
        InnerNode = 0,
        Leaf = 1
    };

    template <typename T = stxxl::uint48>
    class TreeNode {
        constexpr static uint64_t TYPE_MASK = 1llu << (sizeof(T)*8 - 1);
        constexpr static uint64_t DATA_MASK = TYPE_MASK - 1;
        T _data;

#ifdef SEGMENT_TREE_VERBOSE_TOKENS
        TreeNodeType _verbose_type;
        uint64_t _verbose_data;
#endif

    public:
        TreeNode() {}
        TreeNode(const TreeNodeType & type, const uint64_t & data)
              : _data( stxxl::uint64((data & DATA_MASK) | (type * TYPE_MASK)) )
        {
#ifdef SEGMENT_TREE_VERBOSE_TOKENS
            _verbose_type = this->type();
            _verbose_data = this->data();
#endif
            assert(data == (data & DATA_MASK));
        }

        uint64_t data() const {
            return static_cast<stxxl::uint64>(_data) & DATA_MASK;
        }

        TreeNodeType type() const {
            return static_cast<TreeNodeType>(static_cast<bool>(static_cast<stxxl::uint64>(_data) & TYPE_MASK));
        }
    };

    template <typename Output, unsigned int depth=8, unsigned int queue_size = 32, class QueryToken = Token<>, class TreeNode=TreeNode<>>
    class SegmentSubTree {
    protected:
        constexpr static size_t number_of_subtrees = 2<<depth;
        constexpr static size_t number_of_nodes = number_of_subtrees-1;

        Output & _output;
        uint64_t _weight;
        TreeNode _tree[number_of_nodes];
        SegmentSubTree ** _subtrees;

        QueryToken _queue[queue_size];
        unsigned int _queued;

        SegmentSubTree(Output & out) : _output(out) {
            _subtrees = nullptr;
            _queued = 0;
        }

        bool allocate_subtree(unsigned int id) {
            assert(id < number_of_subtrees);

            if (!_subtrees) {
                _subtrees = new SegmentSubTree* [number_of_subtrees];
                std::fill_n(_subtrees, number_of_subtrees, nullptr);
            }

            if (_subtrees[id])
                return false;

            _subtrees[id] = new SegmentSubTree(_output);
            return true;
        }

        void _assign_leaf(unsigned int idx, const TreeNode & token, uint64_t weight) {
            if (idx > number_of_nodes) {
                idx -= number_of_nodes + 1;

                bool new_tree = allocate_subtree(idx);

                // in case we rediscovered this subtree, we have to flush its pending operations
                // (which should reduce its weight to 0) before we can reinitialize it
                if (!new_tree) {
                    _subtrees[idx]->flush(false);
                    assert(!_subtrees[idx]->_weight);
                }

                _subtrees[idx]->_initialize(token, weight);

            } else {
                _tree[idx-1] = token;
            }
        }

        void _process(const QueryToken & token) {
            if (LIKELY(token.type() == TokenType::QuerySegment)) {
                unsigned int node_idx = 1;
                auto token_weight = token.weight();
                _weight--;

                for(unsigned int d=0; d <= depth; d++) {
                    const auto & node = _tree[node_idx - 1];

                    if (UNLIKELY(node.type() == Leaf)) {
                        _output.push(std::make_pair<uint64_t, uint64_t>(node.data(), token.id()));
                        return;
                    }

                    if (token_weight < node.data()) {
                        _tree[node_idx - 1] = TreeNode(TreeNodeType::InnerNode, node.data() - 1);
                        node_idx = 2*node_idx;

                    } else {
                        token_weight -= node.data();
                        node_idx = 2*node_idx + 1;

                    }
                }
                node_idx -= number_of_nodes + 1;

                // if everything worked out correct, we only reach a subtree
                // in case it was earlier constructed using a create segment token
                assert(_subtrees);
                assert(_subtrees[node_idx]);

                _subtrees[node_idx]->_push(QueryToken(TokenType::QuerySegment, token_weight, token.id()));

            } else {
                unsigned int node_idx = 1;
                auto weight = _weight; // weight of the current subtree BEFORE adding new segment
                _weight += token.weight();

                for(unsigned int d=0; d <= depth; d++) {
                    const auto & node = _tree[node_idx - 1];

                    if (UNLIKELY(!weight)) {
                        // This segment is consumed, so we just replace it
                        _tree[node_idx - 1] = TreeNode(Leaf, token.id());

                        return;
                    }

                    if (UNLIKELY(node.type() == Leaf)) {
                        // Split leaf into two segments: one for the old, one for the new value
                        _assign_leaf(2 * node_idx, TreeNode(Leaf, node.data()), weight);
                        _assign_leaf(2 * node_idx + 1, TreeNode(Leaf, token.id()), token.weight());
                        _tree[node_idx - 1] = TreeNode(InnerNode, weight);

                        return;
                    }

                    if (node.data()+token.weight() < weight/2) {
                        // left subtree
                        weight = node.data();
                        _tree[node_idx - 1] = TreeNode(TreeNodeType::InnerNode, weight+token.weight());
                        node_idx = 2*node_idx;

                    } else {
                        // right subtree
                        weight -= node.data();
                        node_idx = 2*node_idx + 1;
                    }
                }

                node_idx -= number_of_nodes + 1;

                assert(node_idx < number_of_subtrees);
                assert(_subtrees);
                assert(_subtrees[node_idx]);

                _subtrees[node_idx]->_push(token);
            }
        }

        void _push(const QueryToken & item) {
            _queue[_queued++] = item;
            if (_queued == queue_size)
                flush(false);
        }

        void _initialize(const TreeNode & root, uint64_t weight) {
            assert(root.type() == TreeNodeType::Leaf);

            _tree[0] = root;
            _weight = weight;
            _queued = 0;
        }

        template <class ostream>
        void _toDot(ostream & os, uint64_t global_id) const {
#ifdef NDEBUG
            os << "not supported with NDEBUG\n";
#else
            std::stack<std::tuple<uint64_t, uint64_t, uint64_t>> stack;
            stack.push(std::make_tuple(global_id, 1, _weight));

            while(!stack.empty()) {
                uint64_t gid = std::get<0>(stack.top());
                uint64_t idx = std::get<1>(stack.top());
                uint64_t weight = std::get<2>(stack.top());
                stack.pop();

                auto &node = _tree[idx - 1];

                if (node.type() == Leaf) {
                    os << " n" << gid << "[shape=box, label=\"id:" << node.data() << ", w:" << weight << "\"]\n";
                } else {
                    os << " n" << gid << "[label=\"" << node.data() << "\"]\n"
                       << " n" << gid << " -> {n" << (2*gid) << ", n" << (2*gid+1) << "}\n";

                    if (2*idx <= number_of_nodes) {
                        stack.push(std::make_tuple(2*gid+1, 2*idx+1, weight - node.data()));
                        stack.push(std::make_tuple(2*gid, 2*idx, node.data()));

                    } else {
                        auto stid = 2*idx - number_of_nodes - 1;

                        assert(_subtrees);

                        if (_subtrees[stid])
                            _subtrees[stid]->_toDot(os, 2*gid);


                        if (_subtrees[stid+1])
                            _subtrees[stid+1]->_toDot(os, 2*gid + 1);

                    }

                }
            }
#endif
        }

    public:
        ~SegmentSubTree() {
            if (_subtrees != nullptr) {
                for (size_t i = 0; i < number_of_subtrees; i++) {
                    if (_subtrees[i])
                        delete (_subtrees[i]);
                }
                delete[] _subtrees;
            }
        }

        void flush(bool recursive = true, bool delete_empty = false) {
            // process queued tokens
            for(unsigned int i = 0; i < _queued; i++) {
                _process(_queue[i]);
            }
            _queued = 0;

            // descent into subtrees (if requested)
            if (recursive && _subtrees) {
                for(unsigned int i=0; i < number_of_subtrees; i++) {
                    if (_subtrees[i]) {
                        _subtrees[i]->flush(true);

                        if (delete_empty && !_subtrees[i]->_weight) {
                            delete _subtrees[i];
                            _subtrees[i] = nullptr;
                        }
                    }
                }
            }
        }
    };

    template <typename Output, unsigned int depth=8, unsigned int queue_size = 32, class QueryToken = Token<>, class TreeNode=TreeNode<>>
    class SegmentTree : public SegmentSubTree<Output, depth, queue_size, QueryToken, TreeNode> {
        using SubTreeType = SegmentSubTree<Output, depth, queue_size, QueryToken, TreeNode>;
        uint64_t _uncached_weight;

    public :
        SegmentTree(Output & out) :
              SubTreeType(out),
              _uncached_weight(0)
        {}

        bool query(uint64_t weight, uint64_t id) {
            if (UNLIKELY(!_uncached_weight))
                return false;

            assert(weight < _uncached_weight);
            this->_push(QueryToken(TokenType::QuerySegment, weight, id));

            --_uncached_weight;

            return true;
        }

        void add_segment(uint64_t weight, uint64_t id) {
            assert(weight);

            if (LIKELY(_uncached_weight)) {
                _uncached_weight += weight;
                this->_push(QueryToken(TokenType::NewSegment, weight, id));

            } else {
                this->flush(true);
                this->_initialize(TreeNode(TreeNodeType::Leaf, id), weight);
                _uncached_weight = weight;

            }
        }

        uint64_t weight() const {
            return _uncached_weight;
        }


        template <class ostream>
        void toDot(ostream & os) const {
#ifdef NDEBUG
            os << "not supported with NDEBUG\n";
#else
            os << "digraph SegTree {\n";
            this->_toDot(os, 1);
            os << "}\n";
#endif
        }
    };
};
