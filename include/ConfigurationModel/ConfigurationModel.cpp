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

//	assert(!_multinodemsg_sorter.empty());
}

void ConfigurationModel::run() {
	_generateMultiNodes();
	
	_multinodemsg_sorter.rewind();
	//_reset();
}
