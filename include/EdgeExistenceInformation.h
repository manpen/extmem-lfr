#pragma once

#include <cassert>
#include <defs.h>
#include <atomic>
#include <thread>
#include <Swaps.h>

class EdgeExistenceInformation {
private:
    std::vector<edge_t> _edges;

    struct swap_info_t {
        std::size_t start_index;
        std::atomic<uint32_t> num_missing_entries;
        std::atomic<uint32_t> num_existing_entries;
    };

    std::vector<swap_info_t> _swap_info;

public:
    EdgeExistenceInformation(swapid_t num_swaps) : _swap_info(num_swaps) {};

    void start_initialization() {
        for (swap_info_t &si : _swap_info) {
            si.num_missing_entries.store(0, std::memory_order_relaxed);
            si.num_existing_entries.store(0, std::memory_order_relaxed);
        }
    };

    void add_possible_info(const swapid_t swap_id, uint32_t num_edges = 1) {
        _swap_info[swap_id].num_missing_entries.fetch_add(num_edges, std::memory_order_relaxed);
    };

    void finish_initialization() {
        std::size_t sum = 0;
        for (swap_info_t &si : _swap_info) {
            si.start_index = sum;
            sum += si.num_missing_entries.load(std::memory_order_consume);
        }
        _edges.resize(sum);
    };

    void push_exists(const swapid_t swap_id, const edge_t& e) {
        swap_info_t &si = _swap_info[swap_id];
        uint32_t i = si.num_existing_entries++;
        _edges[si.start_index + i] = e;
        assert(si.start_index + i < _edges.size() && (static_cast<std::size_t>(swap_id + 1) == _swap_info.size() || si.start_index + i < _swap_info[swap_id + 1].start_index));
        --si.num_missing_entries;
    };

    void push_missing(const swapid_t swap_id) {
        --_swap_info[swap_id].num_missing_entries;
    };

    void wait_for_missing(const swapid_t swap_id) {
        while (_swap_info[swap_id].num_missing_entries.load(std::memory_order_seq_cst) > 0) {
            std::this_thread::yield();
        }
    };

    bool exists(const swapid_t swap_id, const edge_t& e) {
        assert(_swap_info[swap_id].num_missing_entries == 0);
        auto begin_it = _edges.begin() + _swap_info[swap_id].start_index;
        auto end_it = begin_it + _swap_info[swap_id].num_existing_entries;
        // check if linear search is okay here or if we need to sort the existence info
        return (std::find(begin_it, end_it, e) != end_it);
    };
};