#include <stxxl/cmdline>
#include <cstdint>
#include <stdexcept>
#include <vector>
#include <iostream>
#include <fstream>
#include <array>

template <typename stream_t>
uint64_t GetVarint(stream_t &is) {
	auto get_byte = [&is]() -> uint8_t {
		uint8_t result;
		is.read(reinterpret_cast<char*>(&result), 1);
		return result;
	};
	uint64_t u, v = get_byte();
	if (!(v & 0x80)) return v;
	v &= 0x7F;
	u = get_byte(), v |= (u & 0x7F) << 7;
	if (!(u & 0x80)) return v;
	u = get_byte(), v |= (u & 0x7F) << 14;
	if (!(u & 0x80)) return v;
	u = get_byte(), v |= (u & 0x7F) << 21;
	if (!(u & 0x80)) return v;
	u = get_byte(), v |= (u & 0x7F) << 28;
	if (!(u & 0x80)) return v;
	u = get_byte(), v |= (u & 0x7F) << 35;
	if (!(u & 0x80)) return v;
	u = get_byte(), v |= (u & 0x7F) << 42;
	if (!(u & 0x80)) return v;
	u = get_byte(), v |= (u & 0x7F) << 49;
	if (!(u & 0x80)) return v;
	u = get_byte(), v |= (u & 0x7F) << 56;
	if (!(u & 0x80)) return v;
	u = get_byte();
	if (u & 0xFE)
		throw std::overflow_error("Overflow during varint64 decoding.");
	v |= (u & 0x7F) << 63;
	return v;
}

using edge_t = std::pair<uint32_t, uint32_t>;

template <typename stream_t>
class EdgeFileReader {
private:
	stream_t &_is;
	edge_t _cur;
	uint32_t _remaining_deg;
	bool _empty;
public:
	EdgeFileReader(stream_t &is) : _is(is), _remaining_deg(0), _empty(false) {
		if (_is.good()) {
			_remaining_deg = GetVarint(_is);
		}
		operator++();
	}

	EdgeFileReader& operator++() {
		while (_remaining_deg == 0 && _is.good()) {
			_remaining_deg = GetVarint(_is);
			++_cur.first;
		}

		if (_remaining_deg > 0) {
			--_remaining_deg;
			_is.read(reinterpret_cast<char*>(&_cur.second), 4);
		} else {
			_empty = true;
		}

		return *this;
	}

	const edge_t& operator*() const {
		return _cur;
	}

	const edge_t* operator->() const {
		return &_cur;
	}

	bool empty() {
		return _empty;
	}
};

template <typename stream_t>
std::vector<uint32_t> read_partition(stream_t &is) {
	std::vector<uint32_t> result;
	is.seekg(0, std::ios_base::end);
	size_t length = is.tellg() / 8;
	is.seekg(0);
	result.resize(length);
	uint32_t u, p;
	while (is.good()) {
		is.read(reinterpret_cast<char*>(&u), 4);
		is.read(reinterpret_cast<char*>(&p), 4);
		result[u] = p;
	}

	return result;
}

int main(int argc, char* argv[]) {
	std::array<std::string, 2> graph_path,  part_path;
	stxxl::cmdline_parser cp;
	cp.add_param_string("graph_1", graph_path[0], "Path to the first graph");
	cp.add_param_string("part_1", part_path[0], "Path to the first partition");
	cp.add_param_string("graph_2", graph_path[1], "Path to the second graph");
	cp.add_param_string("part_2", part_path[1], "Path to the second partition");

	if (!cp.process(argc, argv)) {
		return 1;
	}

	std::array<std::vector<uint32_t>, 2> p;

	{
		std::ifstream p_stream_0(part_path[0].c_str(), std::ios::binary),
			p_stream_1(part_path[1].c_str(), std::ios::binary);;

		p[0] = read_partition(p_stream_0);
		p[1] = read_partition(p_stream_1);
	}

	std::ifstream g_stream_0(graph_path[0].c_str(), std::ios::binary),
		g_stream_1(graph_path[1].c_str(), std::ios::binary);

	std::array<EdgeFileReader<std::ifstream>, 2> g{{g_stream_0, g_stream_1}};

	uint64_t num_matching_intra_edges = 0, num_matching_inter_edges = 0;
	std::array<uint64_t, 2> num_intra_edges = {{0, 0}}, num_inter_edges = {{0, 0}};

	auto consume = [&](size_t i) {
		if (p[i][g[i]->first] != p[i][g[i]->second]) {
			++num_inter_edges[i];
		} else {
			++num_intra_edges[i];
		}

		++g[i];
	};

	while (!g[0].empty() && !g[1].empty()) {
		if (g[0]->first < g[1]->first) {
			consume(0);
		} else if (g[0]->first > g[1]->first) {
			consume(1);
		} else {
			if (g[0]->second < g[1]->second) {
				consume(1);
			} else if (g[0]->second > g[1]->second) {
				consume(0);
			} else {
				if (p[0][g[0]->first] != p[0][g[0]->second]) {
					++num_matching_inter_edges;
				} else {
					++num_matching_intra_edges;
				}
				consume(0);
				consume(1);
			}
		}
	}

	while (!g[0].empty()) {
		consume(0);
	}

	while (!g[1].empty()) {
		consume(1);
	}


	std::cout << "G1: " << num_intra_edges[0] << " intra-cluster edges, "
		<< num_inter_edges[0] << " inter-cluster edges" << std::endl;
	std::cout << "G2: " << num_intra_edges[1] << " intra-cluster edges, "
		<< num_inter_edges[1] << " inter-cluster edges" << std::endl;
	std::cout << "Matching intra-cluster edges: " << num_matching_intra_edges
		<< " Matching inter-cluster edges: " << num_matching_inter_edges << std::endl;
};
