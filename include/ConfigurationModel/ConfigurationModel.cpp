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
	for(; !_multinodemsg_sorter.empty(); ) {
		auto & fst_node = *_multinodemsg_sorter;	

		++_multinodemsg_sorter;

		MultiNodeMsg snd_node;

		if (!_multinodemsg_sorter.empty()) 
			snd_node = *_multinodemsg_sorter;
		else
			snd_node = fst_node;

		uint64_t fst_nodeid{fst_node.node()};
		uint64_t snd_nodeid{snd_node.node()}; 

		// TODOASK better to normalize here? or later?
		// have to use edge64_t here, def in Header
		if (fst_nodeid < snd_nodeid) 
			_edge_sorter.push(edge64_t{fst_nodeid, snd_nodeid});
		else
			_edge_sorter.push(edge64_t{snd_nodeid, fst_nodeid});

		if (!_multinodemsg_sorter.empty()) 
			++_multinodemsg_sorter;
		else
			break;
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
