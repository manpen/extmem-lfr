/**
 * Hung
 */
#include "ConfigurationModel.h"

#ifdef NDEBUG
void ConfigurationModel::_generateMultiNodes() {
#else
template <class T>
void ConfigurationModel<T>::_generateMultiNodes() {
#endif
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

#ifdef NDEBUG
void ConfigurationModel::_generateSortedEdgeList() {
#else
template <class T>
void ConfigurationModel<T>::_generateSortedEdgeList() {
#endif
	assert(!_multinodemsg_sorter.empty());

	/**
	 * just scan through the permutated multinodes, extract the node only though (wo eid)
	 */
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

#ifdef NDEBUG
void ConfigurationModel::run() {
	_generateMultiNodes();

	assert(!_multinodemsg_sorter.empty());

	_generateSortedEdgeList();

	assert(!_edge_sorter.empty());

	//_reset();
}
#endif
