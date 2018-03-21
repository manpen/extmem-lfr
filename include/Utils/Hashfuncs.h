/**
 * @file HashSingleton.h
 * @date 5. October 2017
 *
 * @author Hung Tran
 */
#pragma once

#include "defs.h"
#include <vector>

namespace Curveball {

template<typename HashClass>
class Hashfuncs {

protected:
	const node_t _num_nodes;
	const uint_t _num_rounds;

	std::vector<HashClass> hash_funcs;
	size_t _shift = 0;

	HashClass _current;
	HashClass _next;

public:

	Hashfuncs(const node_t num_nodes, const uint_t num_rounds) :
					_num_nodes(num_nodes),
					_num_rounds(num_rounds),
					_shift(0)
	{
		hash_funcs.reserve(num_rounds + 1);

		for (uint_t funcid = 0; funcid < num_rounds; funcid++) {
			hash_funcs.emplace_back(HashClass::get_random(num_nodes));
		}
		hash_funcs.emplace_back(HashClass::get_identity(num_nodes));

		// check if last map is the identity
		#ifndef NDEBUG
			for (node_t node = 0; node < num_nodes; node++)
				assert(node == hash_funcs[num_rounds].hash(node));
		#endif

		_current = hash_funcs[_shift];
		_next = hash_funcs[_shift + 1];
	}

	Hashfuncs(Hashfuncs const &) = delete;

	void operator=(Hashfuncs const &) = delete;

	hnode_t current_hash(const node_t node) const {
		assert(_shift <= _num_rounds);

		return _current.hash(node);
	}

	hnode_t next_hash(const node_t node) const {
		assert(_shift + 1 <= _num_rounds);

		return _next.hash(node);
	}

	const Hashfuncs &operator++() {
		assert(_shift < _num_rounds);

		_shift++;
		_current = hash_funcs[_shift];

		if (_shift + 1 <= _num_rounds)
			_next = hash_funcs[_shift + 1];

		return *this;
	}

	const HashClass &operator[](const size_t index) {
		return hash_funcs[index];
	};

	bool at_last() const {
		return _num_rounds == _shift;
	}

	void reset() {
		_shift = 0;
	}
};

}