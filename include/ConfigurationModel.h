#include <limits>

#include <stxxl/vector>
#include <stxxl/sorter>
#include <stxxl/bits/common/uint_types.h>
#include <stxxl/random>

#include <defs.h>
#include <TupleHelper.h>
#include <GenericComparator.h>
#include <Utils/MonotonicPowerlawRandomStream.h>
#include <HavelHakimi/HavelHakimiIMGenerator.h>
#include <EdgeStream.h>

// CRC
#include "nmmintrin.h"

inline uint64_t reverse (const uint64_t & a) {
    uint64_t x = a;
    x = ((x >> 1) & 0x5555555555555555u) | ((x & 0x5555555555555555u) << 1);
    x = ((x >> 2) & 0x3333333333333333u) | ((x & 0x3333333333333333u) << 2);
    x = ((x >> 4) & 0x0F0F0F0F0F0F0F0Fu) | ((x & 0x0F0F0F0F0F0F0F0Fu) << 4);
    x = ((x >> 8) & 0x00FF00FF00FF00FFu) | ((x & 0x00FF00FF00FF00FFu) << 8);
    x = ((x >> 16) & 0x0000FFFF0000FFFFu) | ((x & 0x0000FFFF0000FFFFu) << 16);
    x = ((x >> 32) & 0xFFFFFFFFFu) | ((x & 0xFFFFFFFFFu) << 32);
    return x;
}

inline uint64_t crc64 (const uint32_t & seed, const uint32_t & msb, const uint32_t & lsb) {
    const uint32_t hash_msb_p = _mm_crc32_u32(seed, msb);
    const uint32_t hash_lsb_p = _mm_crc32_u32(hash_msb_p, lsb);
    const uint64_t hash = reverse(static_cast<uint64_t>(hash_msb_p) << 32 | hash_lsb_p);
   
    return hash;
}

constexpr uint64_t NODEMASK = 0x0000000FFFFFFFFFu;
constexpr uint32_t MAX_LSB = 0x9BE09BABu;
constexpr uint32_t MIN_LSB = 0x00000000u;
constexpr uint32_t MAX_CRCFORWARD = 0x641F6454u;

using Edge64Comparator = typename GenericComparator<edge64_t>::Ascending;

/**
 * @typedef multinode_t
 * @brief The default signed integer to be used.
 * 
 * struct for node multiplicity
* in the 36 lsb bits - node
* in the 28 msb bits - key or half_edgeid
* we expect pairwise different representations
*/
class MultiNodeMsg {
public:
    MultiNodeMsg() { }
    MultiNodeMsg(const uint64_t eid_node_) : _eid_node(eid_node_) {}

    using value_type = multinode_t;

    // getters
    uint32_t lsb() const {
        return static_cast<uint32_t>(_eid_node);
    }

    uint32_t msb() const {
        return static_cast<uint32_t>(_eid_node >> 32);
    }

    // just return the node
    multinode_t node() const {
        return _eid_node & NODEMASK;
    }

protected:
    multinode_t _eid_node;
};

// Comparator
class MultiNodeMsgComparator {  
public:
    MultiNodeMsgComparator() {}
    MultiNodeMsgComparator(const uint32_t seed_) 
        : _seed(seed_) 
        , _limits(_setLimits(seed_))
    {
        //std::cout << "WE IN COMP CONSTRUCTOR" << std::endl;
    }

    // invert msb's since lsb = seed then for max_value
    bool operator() (const MultiNodeMsg& a, const MultiNodeMsg& b) const {
        const uint64_t a_hash = crc64(_seed, a.msb(), a.lsb());
        const uint64_t b_hash = crc64(_seed, b.msb(), b.lsb());
       
        return a_hash < b_hash;
    }

    multinode_t max_value() const {
        return _limits.first;
    }

    multinode_t min_value() const {
        return _limits.second;
    }


protected:
    // unnecessary initialization, compiler asks for it
    const uint32_t _seed = 1;
    const std::pair<multinode_t, multinode_t> _limits;

    std::pair<multinode_t, multinode_t> _setLimits(const uint32_t seed_) const {
        multinode_t max_inv_msb = static_cast<multinode_t>(MAX_CRCFORWARD ^ seed_) << 32;
        multinode_t min_inv_msb = static_cast<multinode_t>(seed_) << 32;

        return std::pair<multinode_t, multinode_t>{max_inv_msb | MAX_LSB, min_inv_msb | MIN_LSB};
    }

};

template <class EdgeReader>
class HavelHakimi_ConfigurationModel {
public:
    HavelHakimi_ConfigurationModel() = delete;
    HavelHakimi_ConfigurationModel(const HavelHakimi_ConfigurationModel&) = delete;
    HavelHakimi_ConfigurationModel(EdgeReader & edge_reader_in,
            const uint32_t seed,
            const uint64_t node_upperbound) 
        : _edges(edge_reader_in)
        , _seed(seed)
        , _node_upperbound(node_upperbound)
        , _shift_upperbound(std::min(node_upperbound, _maxShiftBound(node_upperbound)))
        , _multinodemsg_comp(seed)
        , _multinodemsg_sorter(_multinodemsg_comp, SORTER_MEM)
        , _edge_sorter(Edge64Comparator(), SORTER_MEM)
        { }

    using value_type = edge64_t;

    void run() {
        assert(!_edges.empty());

        _generateMultiNodes();

        assert(!_multinodemsg_sorter.empty());

        _generateSortedEdgeList();

        assert(!_edge_sorter.empty());
    }

//! @name STXXL Streaming Interface
//! @{
    bool empty() const {
        return _edge_sorter.empty();
    }

    const value_type& operator*() const {
        //assert(!_edge_sorter.empty());

        return *_edge_sorter;
    }

    HavelHakimi_ConfigurationModel&operator++() {
        assert(!_edge_sorter.empty());
        
        ++_edge_sorter;

        return *this;
    }
//! @}

    void clear() {
        _multinodemsg_sorter.clear();
        _edge_sorter.clear();
    }

    uint64_t size(){
        return _edge_sorter.size();
    }

protected:
    EdgeReader _edges;

    const uint32_t _seed;
    const uint64_t _node_upperbound;
    const uint64_t _shift_upperbound;

    typedef stxxl::sorter<MultiNodeMsg, MultiNodeMsgComparator> MultiNodeSorter;
    MultiNodeMsgComparator _multinodemsg_comp;
    MultiNodeSorter _multinodemsg_sorter;

    using EdgeSorter = stxxl::sorter<value_type, Edge64Comparator>;
    EdgeSorter _edge_sorter; 

    void _generateMultiNodes() {
        assert(!_edges.empty());

        stxxl::random_number<> rand;

        multinode_t prev_node = INVALID_MULTINODE; // = (*_edges).first;
        uint64_t shift = rand(_shift_upperbound);

        for (; !_edges.empty(); ++_edges) {
            _multinodemsg_sorter.push(
                MultiNodeMsg{ ((static_cast<multinode_t>(_edges.edge_ids().first) * (_node_upperbound | 1) + shift) << 36) | (*_edges).first});
            _multinodemsg_sorter.push(
                MultiNodeMsg{ ((static_cast<multinode_t>(_edges.edge_ids().second) * (_node_upperbound | 1) + shift)  << 36) | (*_edges).second});

            if ((*_edges).first == prev_node) {
                shift = rand(_shift_upperbound);
            }

            prev_node = (*_edges).first;
        }

        _multinodemsg_sorter.sort();

        assert(!_multinodemsg_sorter.empty());
    }

    /**
     * HavelHakimi gives us a graphical sequence, therefore no need to randomize an "half-edge" for the last node.
    **/
    void _generateSortedEdgeList() {
        assert(!_multinodemsg_sorter.empty());

        for(; !_multinodemsg_sorter.empty(); ++_multinodemsg_sorter) {
            auto & fst_node = *_multinodemsg_sorter;

            ++_multinodemsg_sorter;

            auto & snd_node = *_multinodemsg_sorter;

            if (fst_node.node() < snd_node.node())
                _edge_sorter.push(edge64_t{fst_node.node(), snd_node.node()});
            else
                _edge_sorter.push(edge64_t{snd_node.node(), fst_node.node()});
        }

        _edge_sorter.sort();
    }

    uint64_t _maxShiftBound(uint64_t n) const {
        return 27 - static_cast<uint64_t>(log2(n));
    }
};

template <typename T = MonotonicPowerlawRandomStream<false>>
class ConfigurationModel {
public:
    ConfigurationModel() = delete; 

    ConfigurationModel(const ConfigurationModel&) = delete;
    ConfigurationModel(T &degrees, const uint32_t seed, const uint64_t node_upperbound)
                                : _degrees(degrees) 
                                , _seed(seed)
                                , _node_upperbound(node_upperbound)
                                , _multinodemsg_comp(seed)
                                , _multinodemsg_sorter(_multinodemsg_comp, SORTER_MEM)
                                , _edge_sorter(Edge64Comparator(), SORTER_MEM)
    { }

    // implements execution of algorithm
    void run() {
        const uint64_t node_size = _generateMultiNodes();

        assert(!_multinodemsg_sorter.empty());

        _generateSortedEdgeList(node_size);

        assert(!_edge_sorter.empty());
    }

//! @name STXXL Streaming Interface
//! @{
    bool empty() const {
        return _edge_sorter.empty();
    }

    const edge64_t& operator*() const {
        assert(!_edge_sorter.empty());

        return *_edge_sorter;
    }

    ConfigurationModel&operator++() {
        assert(!_edge_sorter.empty());
        
        ++_edge_sorter;

        return *this;
    }
//! @}

    // for testing
    void clear() {
        _reset();
    }

protected:
    T _degrees;
    const uint32_t _seed;
    const uint64_t _node_upperbound;

    typedef stxxl::sorter<MultiNodeMsg, MultiNodeMsgComparator> MultiNodeSorter;
    MultiNodeMsgComparator _multinodemsg_comp;
    MultiNodeSorter _multinodemsg_sorter;

    using EdgeSorter = stxxl::sorter<edge64_t, Edge64Comparator>;
    EdgeSorter _edge_sorter; 

    // internal algos
    uint64_t _generateMultiNodes() {
        assert(!_degrees.empty());
        stxxl::random_number<> rand;
        uint64_t count = 1;

        for (; !_degrees.empty(); ++_degrees, ++count) {
            uint64_t multiplicity = (static_cast<uint64_t>(*_degrees) | 1);
            for (degree_t j = 1; j <= *_degrees; ++j) {
               _multinodemsg_sorter.push(MultiNodeMsg{((static_cast<multinode_t>(j) * (_node_upperbound | 1) * multiplicity) << 36) | count});
            }
        }
            _multinodemsg_sorter.sort();

            assert(!_multinodemsg_sorter.empty());
        
            return count;
        }
        
        void _generateSortedEdgeList(const uint64_t node_size) {
            assert(!_multinodemsg_sorter.empty());

            for(; !_multinodemsg_sorter.empty(); ) {
                auto & fst_node = *_multinodemsg_sorter;

                ++_multinodemsg_sorter;

                if (LIKELY(!_multinodemsg_sorter.empty())) {
                    MultiNodeMsg snd_node = *_multinodemsg_sorter;

                    if (fst_node.node() < snd_node.node())
                        _edge_sorter.push(edge64_t{fst_node.node(), snd_node.node()});
                    else
                        _edge_sorter.push(edge64_t{snd_node.node(), fst_node.node()});
                } else {
                    stxxl::random_number<> rand;
                    const uint64_t snd_nodeid = rand(node_size);

                    if (fst_node.node() < snd_nodeid)
                        _edge_sorter.push(edge64_t{fst_node.node(), snd_nodeid});
                    else
                        _edge_sorter.push(edge64_t{snd_nodeid, fst_node.node()});
                }

                if (!_multinodemsg_sorter.empty())
                    ++_multinodemsg_sorter;
                else
                    break;
            }

            _edge_sorter.sort();

        }
        
        void _reset() {
            _multinodemsg_sorter.clear();
            _edge_sorter.clear();
        }
};

// Pseudo-random approach

struct TestNodeMsg {
    multinode_t key;
    multinode_t node;

    TestNodeMsg() { }
    TestNodeMsg(const multinode_t &key_, const multinode_t &node_) : key(key_), node(node_) {}

    DECL_LEX_COMPARE_OS(TestNodeMsg, key, node);
};



using TestNodeComparator = typename GenericComparatorStruct<TestNodeMsg>::Ascending;
using TestNodeSorter = stxxl::sorter<TestNodeMsg, TestNodeComparator>;

template <typename EdgeReader>
class HavelHakimi_ConfigurationModel_Random {
public:
    HavelHakimi_ConfigurationModel_Random() = delete; 

    HavelHakimi_ConfigurationModel_Random(const HavelHakimi_ConfigurationModel_Random&) = delete;
    HavelHakimi_ConfigurationModel_Random(EdgeReader &edges, const uint64_t node_upperbound)
                                : _edges(edges)
                                , _node_upperbound(node_upperbound)
                                , _testnode_sorter(TestNodeComparator{}, SORTER_MEM)
                                , _test_edge_sorter(Edge64Comparator(), SORTER_MEM)
    { }

    using value_type = edge64_t;

    // implements execution of algorithm
    void run() {
        assert(!_edges.empty());

        _generateMultiNodes();

        assert(!_testnode_sorter.empty());

        _generateSortedEdgeList();

        assert(!_test_edge_sorter.empty());
    }

//! @name STXXL Streaming Interface
//! @{
    bool empty() const {
        return _test_edge_sorter.empty();
    }

    const edge64_t& operator*() const {
        assert(!_test_edge_sorter.empty());

        return *_test_edge_sorter;
    }

    HavelHakimi_ConfigurationModel_Random&operator++() {
        assert(!_test_edge_sorter.empty());

        ++_test_edge_sorter;
        
        return *this;
    }
//! @}
    
    // for testing
    void clear() {
        _testnode_sorter.clear();
        _test_edge_sorter.clear();
    }

    uint64_t size() {
        return _test_edge_sorter.size();
    }

protected:
    EdgeReader _edges;
    const uint64_t _node_upperbound;

    TestNodeSorter _testnode_sorter;

    using EdgeSorter = stxxl::sorter<value_type, Edge64Comparator>;
    EdgeSorter _test_edge_sorter;
    // internal algos
    void _generateMultiNodes() {
        assert(!_edges.empty());

        stxxl::random_number64 rand64;
       
        for (; !_edges.empty(); ++_edges) {

            _testnode_sorter.push(TestNodeMsg{rand64(), static_cast<multinode_t>((*_edges).first)});
            _testnode_sorter.push(TestNodeMsg{rand64(), static_cast<multinode_t>((*_edges).second)});

        }
       
        _testnode_sorter.sort();

        assert(!_testnode_sorter.empty());
    }
    
    // HavelHakimi gives us a graphical degree sequence, no need for randomization for last node
    void _generateSortedEdgeList() {
        assert(!_testnode_sorter.empty());

        for(; !_testnode_sorter.empty(); ++_testnode_sorter) {
            auto & fst_node = *_testnode_sorter;

            ++_testnode_sorter;

            auto & snd_node = *_testnode_sorter;

            if (fst_node.node < snd_node.node)
                _test_edge_sorter.push(edge64_t{fst_node.node, snd_node.node});
            else
                _test_edge_sorter.push(edge64_t{snd_node.node, fst_node.node});
        }

        _test_edge_sorter.sort();
    }
};
