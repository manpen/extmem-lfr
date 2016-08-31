/**
 * Hung
 */
#include "ConfigurationModel.h"

void ConfigurationModel::_generateMultiNodes() {
	assert(!_degrees.empty());
	
	uint64_t i = 1;
	uint64_t j;

	for (; !_degrees.empty(); ++i, ++_degrees) {
		for (j = 0; static_cast<int32_t>(j) < *_degrees; ++j)
			_multinodemsg_sorter.push(MultiNodeMsg{(j << 36) | i});
	}

	_multinodemsg_sorter.sort();

	assert(!_multinodemsg_sorter.empty());
}

#ifndef NDEBUG
void ConfigurationModel::_generateSortedEdgeList() {
	assert(!_multinodemsg_sorter.empty());

	/**
	 * just scan through the permutated multinodes, extract the node only though (wo eid)
	 */
	// TODOASK support 36bit container? 4.5Bytes...? 
	// get first node 
	// TODOASK should the last node be connected to the first?
	auto & prev_node = *_multinodemsg_sorter;
	uint64_t prev_nodeid{prev_node.node()};
	++_multinodemsg_sorter;

	// scan through rest
	for(; !_multinodemsg_sorter.empty(); ++_multinodemsg_sorter) {
		auto & recent_node = *_multinodemsg_sorter;

		uint64_t recent_nodeid{recent_node.node()};

		// TODOASK better to normalize here? or later?
		// have to use edge64_t here, def in Header
		if (recent_nodeid < prev_nodeid) 
			_edge_sorter.push(edge64_t{recent_nodeid, prev_nodeid});
		else
			_edge_sorter.push(edge64_t{prev_nodeid, recent_nodeid});

		prev_nodeid = recent_nodeid;
	}

	_edge_sorter.sort();
}
#endif

void ConfigurationModel::run() {
	_generateMultiNodes();

	assert(!_multinodemsg_sorter.empty());

	_generateSortedEdgeList();

	assert(!_edge_sorter.empty());

	//_reset();
}
