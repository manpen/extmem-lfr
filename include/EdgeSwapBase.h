#pragma once
#include <Swaps.h>
#include <defs.h>

#include <sstream>
#include <string>

class EdgeSwapBase {
public:
    using edge_vector = stxxl::vector<edge_t>;
    using swap_descriptor = SwapDescriptor;
    using swap_vector = stxxl::vector<swap_descriptor>;

#ifdef EDGE_SWAP_DEBUG_VECTOR
    using debug_vector = stxxl::vector<SwapResult>;
#endif

protected:
#ifdef EDGE_SWAP_DEBUG_VECTOR
    debug_vector _result;
#endif

    bool _display_debug;

    std::pair<edge_t, edge_t> _swap_edges(const edge_t & e0, const edge_t & e1, bool direction) const {
        edge_t t0, t1;
        if (direction) {
            if (e0.second < e1.first) {
                t0 = {e0.second, e1.first};
            } else {
                t0 = {e1.first, e0.second};
            }
            if (e0.first < e1.second) {
                t1 = {e0.first, e1.second};
            } else {
                t1 = {e1.second, e0.first};
            }
        } else {
            if (e1.first < e0.first) {
                t0 = {e1.first, e0.first};
            } else {
                t0 = {e0.first, e1.first};
            }
            if (e0.second < e1.second) {
                t1 = {e0.second, e1.second};
            } else {
                t1 = {e1.second, e0.second};
            }
        }

        return std::make_pair(t0, t1);
    }

    stxxl::stats_data _stats;

    //! Start a statistics counter that will be reported using _report_stats
    void _start_stats(bool compute_stats = true) {
        if (!compute_stats) return;
        _stats = stxxl::stats_data(*stxxl::stats::get_instance());
    }

    //! Report I/O and time statistics and restarts counters
    //! @warning Function reports only relative values, so call _start_stats before using
    void _report_stats(const std::string &prefix, bool compute_stats = true) {
        if (!compute_stats) return;

        auto start = _stats;
        _start_stats();
        std::ostringstream ss;
        ss << (_stats - start);

        std::string str = ss.str();
        std::string replace = "\n" + prefix;
        str = prefix + str;

        size_t pos = prefix.size();
        while (1) {
            pos = str.find("\n", pos);
            if (pos == std::string::npos) break;
            str.replace(pos, 1, replace);
            pos += replace.length();
        }

        std::cout << str << std::endl;
    }

public:
    EdgeSwapBase() :
          _display_debug(false)
    {}

    void setDisplayDebug(bool v) {
        _display_debug = v;
    }

#ifdef EDGE_SWAP_DEBUG_VECTOR
    //! The i-th entry of this vector corresponds to the i-th
    //! swap provided to the constructor
    debug_vector &debugVector() {
        return _result;
    }
#endif
};
