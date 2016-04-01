#pragma once

#include <string>
#include <sstream>
#include <stxxl/stats>

class IOStatistics {
    std::string _prefix;

    stxxl::stats& _stats;
    stxxl::stats_data _begin;

public:
    IOStatistics(stxxl::stats & stats = *stxxl::stats::get_instance())
          : _stats(stats), _begin(stats)
    {}

    IOStatistics(const std::string& prefix, stxxl::stats & stats = *stxxl::stats::get_instance())
          : _prefix(prefix), _stats(stats), _begin(stats)
    {}

    ~IOStatistics() {
        if (!_prefix.empty())
            report();
    }

    void start() {
        _begin = stxxl::stats_data(_stats);
    }


    void report() const {
        if (_prefix.empty()) {
            std::cout << (stxxl::stats_data(_stats) - _begin) << std::endl;
        } else {
            report(_prefix);
        }
    }

    void report(const std::string & prefix) const {
        std::ostringstream ss;
        ss << (stxxl::stats_data(_stats) - _begin);

        std::string str = ss.str();
        std::string replace = "\n" + prefix;
        replace += ": ";

        size_t pos = 0;
        while (1) {
            pos = str.find("\n", pos);
            if (pos == std::string::npos) break;

            str.replace(pos, 1, replace);
            pos += replace.length();
        }

        std::cout << prefix << ": " << str << std::endl;
    }
};
