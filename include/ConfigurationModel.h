#include <limits>

#include <stxxl/vector>
#include <stxxl/sorter>
#include <stxxl/bits/common/uint_types.h>
#include <stxxl/random>

#include <defs.h>
#include <TupleHelper.h>
#include <GenericComparator.h>
#include <Utils/MonotonicPowerlawRandomStream.h>

// CRC
#include "nmmintrin.h"

uint64_t reverse (const uint64_t & a) {
    uint64_t x = a;
    x = ((x >> 1) & 0x5555555555555555u) | ((x & 0x5555555555555555u) << 1);
    x = ((x >> 2) & 0x3333333333333333u) | ((x & 0x3333333333333333u) << 2);
    x = ((x >> 4) & 0x0F0F0F0F0F0F0F0Fu) | ((x & 0x0F0F0F0F0F0F0F0Fu) << 4);
    x = ((x >> 8) & 0x00FF00FF00FF00FFu) | ((x & 0x00FF00FF00FF00FFu) << 8);
    x = ((x >> 16) & 0x0000FFFF0000FFFFu) | ((x & 0x0000FFFF0000FFFFu) << 16);
    x = ((x >> 32) & 0xFFFFFFFFFu) | ((x & 0xFFFFFFFFFu) << 32);
    return x;
}

uint64_t crc64 (const uint32_t & seed, const uint32_t & msb, const uint32_t & lsb) {
    const uint32_t hash_msb_p = _mm_crc32_u32(seed, msb);
    const uint32_t hash_lsb_p = _mm_crc32_u32(hash_msb_p, lsb);
    const uint64_t hash = reverse(static_cast<uint64_t>(hash_msb_p) << 32 | hash_lsb_p);
   
    return hash;
}

constexpr uint64_t NODEMASK = 0x0000000FFFFFFFFF;
constexpr uint32_t MAX_LSB = 0x9BE09BAB;
constexpr uint32_t MIN_LSB = 0x00000000;
constexpr uint32_t MAX_CRCFORWARD = 0x641F6454;

using multinode_t = uint64_t;

//! Type for every (un)directed 64bit
// ommited invalid() member function
struct edge64_t : public std::pair<multinode_t, multinode_t> {
    edge64_t() : std::pair<multinode_t, multinode_t>() {}
    edge64_t(const std::pair<multinode_t, multinode_t> & edge) : std::pair<multinode_t, multinode_t>(edge) {}
    edge64_t(const multinode_t & v1, const multinode_t & v2) : std::pair<multinode_t, multinode_t>(v1, v2) {}

    //! Enforces first<=second
    void normalize() {
        if (first > second)
            std::swap(first, second);
    }

    //! Returns true if edge represents a self-loop
    bool is_loop() const {
        return first == second;
    }
};

struct Edge64Comparator {
    bool operator()(const edge64_t &a, const edge64_t &b) const {
        if (a.first == b.first) 
            return a.second < b.second;
        else
            return a.first < b.first;
    }

    edge64_t min_value() const {
        return edge64_t(std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::min());
    }

    edge64_t max_value() const {
        return edge64_t(std::numeric_limits<uint64_t>::max(), std::numeric_limits<uint64_t>::max());
    }
};

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

    // getters
    uint32_t lsb() const {
        return static_cast<uint32_t>(_eid_node);
    }

    uint32_t msb() const {
        return static_cast<uint32_t>(_eid_node >> 32);
    }

    // just return the node
    uint64_t node() const {
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

    uint64_t max_value() const {
        return _limits.first;
    }

    uint64_t min_value() const {
        return _limits.second;
    }


protected:
    // unnecessary initialization, compiler asks for it
    const uint32_t _seed = 1;
    const std::pair<uint64_t,uint64_t> _limits;

    std::pair<multinode_t, multinode_t> _setLimits(const uint32_t seed_) const {
        uint64_t max_inv_msb = static_cast<uint64_t>(MAX_CRCFORWARD ^ seed_) << 32;
        uint64_t min_inv_msb = static_cast<uint64_t>(0x00000000 ^ seed_) << 32;

        return std::pair<multinode_t, multinode_t>{reverse(max_inv_msb | MAX_LSB), reverse(min_inv_msb | MIN_LSB)};
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

                //std::cout << "NODE:\t\t" << std::dec << fst_node.node() << "\t\t, HASH:\t\t" << std::hex << crc64(_seed, fst_node.msb(), fst_node.lsb()) << std::endl;

                ++_multinodemsg_sorter;

                if (LIKELY(!_multinodemsg_sorter.empty())) {
                    MultiNodeMsg snd_node = *_multinodemsg_sorter;

                    //std::cout << "NODE:\t\t" << std::dec << snd_node.node() << "\t\t, HASH:\t\t" << std::hex << crc64(_seed, snd_node.msb(), snd_node.lsb()) << std::endl;

                    //std::cout << "NEW EDGE: <" << fst_node.node() << "," << snd_node.node() << ">" << std::endl;

                    /*if (fst_node.node() == snd_node.node()) {
                        std::cout << "NEW EDGE: <" << fst_node.node() << "," << snd_node.node() << ">" << std::endl;
                        std::cout << "NODE:\t\t" << std::dec << fst_node.node() << "\t\t, HASH:\t\t" << std::hex << crc64(_seed, fst_node.msb(), fst_node.lsb()) << std::endl;
                        std::cout << "NODE:\t\t" << std::dec << snd_node.node() << "\t\t, HASH:\t\t" << std::hex << crc64(_seed, snd_node.msb(), snd_node.lsb()) << std::endl;
                    }*/

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

inline std::ostream &operator<<(std::ostream &os, const edge64_t & t) {
    os << "edge64(" << t.first << "," << t.second << ")";
    return os;
}

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

template <typename T = MonotonicPowerlawRandomStream<false>>
class ConfigurationModelRandom {
public:
    ConfigurationModelRandom() = delete; 

    ConfigurationModelRandom(const ConfigurationModelRandom&) = delete;
    ConfigurationModelRandom(T &degrees, const uint32_t seed, const uint64_t node_upperbound)
                                : _degrees(degrees) 
                                , _seed(seed)
                                , _node_upperbound(node_upperbound)
                                , _testnode_sorter(TestNodeComparator{}, SORTER_MEM)
                                , _test_edge_sorter(Edge64Comparator(), SORTER_MEM)
    { }

    // implements execution of algorithm
    void run() {
        const uint64_t node_size = _generateMultiNodes();

        assert(!_testnode_sorter.empty());

        _generateSortedEdgeList(node_size);

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

    ConfigurationModelRandom&operator++() {
        assert(!_test_edge_sorter.empty());

        ++_test_edge_sorter;
        
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

    TestNodeSorter _testnode_sorter;

    using EdgeSorter = stxxl::sorter<edge64_t, Edge64Comparator>;
    EdgeSorter _test_edge_sorter;
    // internal algos
    uint64_t _generateMultiNodes() {
        assert(!_degrees.empty());
        stxxl::random_number64 rand64;
        stxxl::random_number<> rand;
        uint64_t count = 1;

        for (; !_degrees.empty(); ++_degrees, ++count) {
            for (degree_t j = 1; j <= *_degrees; ++j) {
                _testnode_sorter.push(TestNodeMsg{rand64(), count});
            }
        }
            _testnode_sorter.sort();

            assert(!_testnode_sorter.empty());
        
            return count;
        }
        
        void _generateSortedEdgeList(const uint64_t node_size) {
            assert(!_testnode_sorter.empty());

            for(; !_testnode_sorter.empty(); ) {
                auto & fst_node = *_testnode_sorter;

                ++_testnode_sorter;

                if (LIKELY(!_testnode_sorter.empty())) {
                    TestNodeMsg snd_node = *_testnode_sorter;

                    if (fst_node.node < snd_node.node)
                        _test_edge_sorter.push(edge64_t{fst_node.node, snd_node.node});
                    else
                        _test_edge_sorter.push(edge64_t{snd_node.node, fst_node.node});
                } else {
                    stxxl::random_number<> rand;
                    const uint64_t snd_nodeid = rand(node_size);

                    if (fst_node.node < snd_nodeid)
                        _test_edge_sorter.push(edge64_t{fst_node.node, snd_nodeid});
                    else
                        _test_edge_sorter.push(edge64_t{snd_nodeid, fst_node.node});
                }

                if (!_testnode_sorter.empty())
                    ++_testnode_sorter;
                else
                    break;
            }

            _test_edge_sorter.sort();
        }
        
        void _reset() {
            _testnode_sorter.clear();
            _test_edge_sorter.clear();
        }
};
