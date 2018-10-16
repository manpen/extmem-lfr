/**
 * @file NodeHash.h
 * @date 28. September 2017
 *
 * @author Hung Tran
 */

#pragma once

#include "defs.h"
#include <random>

namespace Curveball {

	static node_t inverse_of_in(node_t a, node_t p) {
		node_t t = 0;
		node_t new_t = 1;
		node_t r = p;
		node_t new_r = a;

		while (new_r != 0) {
			node_t tmp;
			const auto quot = static_cast<node_t>(r / new_r);

			tmp = t;
			t = new_t;
			new_t = tmp - quot * new_t;
			tmp = r;
			r = new_r;
			new_r = tmp - quot * new_r;
		}
		if (t < 0)
			return t + p;
		else
			return static_cast<node_t>(t);
	}

	static bool is_prime(const node_t n) {
		if (n <= 1)
			return false;
		else if (n <= 3)
			return true;
		else if (n % 2 == 0 || n % 3 == 0)
			return false;
		node_t tmp = 5;
		while (tmp * tmp <= n) {
			if (n % tmp == 0 || n % (tmp + 2) == 0)
				return false;
			tmp = tmp + 6;
		}

		return true;
	}

	static uint32_t get_next_prime(const node_t num_nodes) {
		uint32_t next_prime = num_nodes + 1;

		if (next_prime <= 2)
			next_prime = 2;

		if (next_prime % 2 == 0)
			next_prime++;

		for (; !is_prime(next_prime); next_prime += 2);

		assert(next_prime > num_nodes);

		return next_prime;
	}

	class ModHash {
	private:
		node_t _a;
		node_t _ainv;
		node_t _b;
		uint32_t _p;
	public:
		ModHash() = default;

		ModHash(const ModHash&) = default;

		ModHash& operator = (const ModHash&) = default;

		ModHash(ModHash&&) = default;

		ModHash& operator = (ModHash&&) = default;

		ModHash(const node_t a, const node_t b, const uint32_t p) :
			_a(a % p),
			_ainv(inverse_of_in(a, p)),
			_b(b % p),
			_p(p) {
			assert(_ainv > 0);
			assert(a > 0);
			assert(b < p);
		}

		hnode_t hash(const node_t node) const {
			return (_a * node + _b) % _p;
		}

		node_t invert(const hnode_t hnode) const {
			return ((hnode - _b) * _ainv) % _p;
		}

		bool operator()(const node_t &a, const node_t &b) const {
			return hash(a) < hash(b);
		}

		hnode_t min_value() const {
			return ((_p - _b) * _ainv) % _p;
		}

		hnode_t max_value() const {
			return ((_p - _b - 1) * _ainv) % _p;
		}

		static ModHash get_random(const node_t num_nodes) {
			const uint32_t next_prime = get_next_prime(num_nodes);

			std::random_device rd;
			STDRandomEngine gen(rd());
			std::uniform_int_distribution<node_t> dis(1, next_prime - 1);

			return ModHash{dis(gen), dis(gen), next_prime};
		}

		static ModHash get_identity(const node_t num_nodes) {
			return ModHash{1, 0, DUMMY_PRIME};
		}
	};

}