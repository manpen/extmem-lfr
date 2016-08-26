#pragma once
#include <stxxl/vector>
#include <stxxl/sorter>

#include "GenericComparator.h"
#include "TupleHelper.h"
#include "Utils/MonotonicPowerlawRandomStream.h"

#include <defs.h>

/**
 * @typedef multinode_t
 * @brief The default signed integer to be used.
 * 
 * struct for node multiplicity
 * in the 36 lsb bits - node
 * in the 28 msb bits - key
 * we expect pairwise different representations
 */
// the key_node is the payload for the sorter
struct MultiNodeMsg {
	uint64_t crc_hash;
	uint64_t key_node;

	MultiNodeMsg() { }
	MultiNodeMsg(const uint64_t &crc_hash_, const uint64_t key_node_) : crc_hash(crc_hash_), key_node(key_node_) {}

	//bool operator< (const MultiNodeMsg& o) const {
	//	return (crc_hash < o.crc_hash || (crc_hash == o.crc_hash && (key_node > o.key_node)));
	//}

	//DECL_TO_TUPLE(crc_hash, key_node);
	//DECL_TUPLE_OS(MultiNodeMsg);
	DECL_LEX_COMPARE_OS(MultiNodeMsg, crc_hash, key_node);
};

using MultiNodeMsgComparator = typename GenericComparatorStruct<MultiNodeMsg>::Ascending;
using MultiNodeMsgSorter = stxxl::sorter<MultiNodeMsg, MultiNodeMsgComparator>;

void generate();
void testStruct();
void testSorter();
