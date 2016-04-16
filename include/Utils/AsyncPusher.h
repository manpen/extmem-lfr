#pragma once

#include<vector>
#include<list>
#include<thread>
#include<future>

#define ASYNC_PUSHER_STATS

template <class TargetT, typename T>
class AsyncPusher {
    using BufferType = std::vector<T>;
    using BufIt = typename BufferType::iterator;

    // Target and Parameters
    TargetT & _target;
    const size_t _buffer_size;
    const unsigned int _number_of_buffers;

    // Buffer stores
    std::list<std::unique_ptr<BufferType>> _target_buffers;
    std::list<std::unique_ptr<BufferType>> _empty_buffers;

    // Push interface
    BufIt _push_it;
    BufIt _push_end;

    // Multi-Threading
    std::future<void> _pusher_done;
    std::mutex _list_mutex;
    std::condition_variable _target_cv;
    std::condition_variable _empty_cv;
    std::atomic_bool _done_pushing;

    // Statistics
#ifdef ASYNC_PUSHER_STATS
    uint64_t _stat_wait_for_empty;
    uint64_t _stat_wait_for_filled;
    uint64_t _stat_number_pushes;
#endif

#ifndef NDEBUG
    uint64_t _stat_items_received;
    uint64_t _stat_items_pushed_to_target;
#endif

    void _fetch_push_buffer(bool push_current) {
        #ifdef ASYNC_PUSHER_STATS
            auto begin = std::chrono::high_resolution_clock::now();
        #endif
        { // CRITICAL_SECTION
            std::unique_lock <std::mutex> lck(_list_mutex);

            if (push_current) {
                // move the most recently filled buffer into the target list
                _target_buffers.emplace_back(nullptr);
                std::swap(_target_buffers.back(), _empty_buffers.front());
                _empty_buffers.pop_front();

                // wake pusher thread
                _target_cv.notify_one();
            }

            // wait for a buffer
            _empty_cv.wait(lck, [&]() { return !_empty_buffers.empty(); });
        }
        #ifdef ASYNC_PUSHER_STATS
            auto end = std::chrono::high_resolution_clock::now();
            _stat_wait_for_empty += std::chrono::duration_cast<std::chrono::milliseconds>(end-begin).count();
            _stat_number_pushes+= push_current;
        #endif


        // fetch the first one
        BufferType & buf = *_empty_buffers.front();
        _push_it = buf.begin();
        _push_end = buf.end();
    }

    void _pusher_main() {
        bool first_iteration = true;
        while(1) {
            #ifdef ASYNC_PUSHER_STATS
                auto begin = std::chrono::high_resolution_clock::now();
            #endif
            {
                std::unique_lock<std::mutex> lck(_list_mutex);

                if (!first_iteration) {
                    // we need to return the buffer
                    _empty_buffers.emplace_back(nullptr);
                    std::swap(_target_buffers.front(), _empty_buffers.back());
                    _target_buffers.pop_front();

                    // wake a lazy producer
                    _empty_cv.notify_one();
                }

                // wait for buffer to push
                _target_cv.wait(lck, [&]() { return !_target_buffers.empty() || _done_pushing; });
            }
            #ifdef ASYNC_PUSHER_STATS
                auto end = std::chrono::high_resolution_clock::now();
                _stat_wait_for_filled += std::chrono::duration_cast<std::chrono::milliseconds>(end-begin).count();
            #endif

            // stop here
            if (_target_buffers.empty()) {
                assert(_done_pushing);
                assert(_stat_items_received == _stat_items_pushed_to_target);

                std::cout << "Bye" << std::endl;
                return;
            }

            // push into target
            for(const auto & e : *_target_buffers.front()) {
                _target.push(e);
                #ifndef NDEBUG
                    _stat_items_pushed_to_target++;
                #endif
            }


            first_iteration = false;
        }
    }

public:
    AsyncPusher(TargetT& target, size_t elements_in_buffer = 2llu << 20, unsigned int number_of_buffers = 3)
        : _target(target)
        , _buffer_size(elements_in_buffer)
        , _number_of_buffers(number_of_buffers)
        , _done_pushing(false)
        , _stat_wait_for_empty(0)
        , _stat_wait_for_filled(0)
        , _stat_number_pushes(0)
#ifndef NDEBUG
        , _stat_items_received(0)
        , _stat_items_pushed_to_target(0)
#endif
    {
        assert(_buffer_size > 0);
        assert(_number_of_buffers > 1);

        for(; number_of_buffers; number_of_buffers--) {
            _empty_buffers.emplace_back( new BufferType(elements_in_buffer) );
        }

        _fetch_push_buffer(false);

        _pusher_done = std::async(std::launch::async, &AsyncPusher::_pusher_main, this);
    }

    ~AsyncPusher() {
        if (!_done_pushing) {
            std::cerr << "Terminate non-finished AsyncPusher. Wait for completion." << std::endl;
            finish();
        }

        waitForPusher();
    }

    void push(const T & data) {
        assert(_push_it != _push_end);
        *_push_it = data;

        #ifndef NDEBUG
        _stat_items_received++;
        #endif

        if (UNLIKELY((++_push_it == _push_end)))
            _fetch_push_buffer(true);
    }

    void finish(bool wait = true) {
        // push last buffer if necessary
        {
            assert(!_empty_buffers.empty());
            auto & buf = *_empty_buffers.front();

            ptrdiff_t elements_pushed = _push_it - buf.begin();
            assert(elements_pushed < buf.size());

            if (elements_pushed) {
                buf.resize(elements_pushed);
                _fetch_push_buffer(true);
            }
        }

        _done_pushing = true;
        _target_cv.notify_one();

        if (wait)
            waitForPusher();
    }

    void waitForPusher() {
        assert(_done_pushing);
        if (_pusher_done.valid())
            _pusher_done.get();

        assert(_stat_items_received == _stat_items_pushed_to_target);
    }

    void restart() {
        waitForPusher();

        assert(_target_buffers.empty());
        assert(_empty_buffers.size() == _number_of_buffers);

        for(auto & ptr : _empty_buffers) {
            ptr->resize(_buffer_size);
        }

        _pusher_done = std::async(std::launch::async, &AsyncPusher::_pusher_main, this);
    }

    void report_stats(const std::string & name) {
        #ifdef ASYNC_PUSHER_STATS
            std::cout << name << ": ";
            report_stats();
        #endif
    }

    void report_stats() {
        #ifdef ASYNC_PUSHER_STATS
            std::cout << "Wait time for empty buffers: " << _stat_wait_for_empty << "ms "
                         "for filled buffers " << _stat_wait_for_filled << "ms. "
                         "Performed " << _stat_number_pushes << " pushs"
            #ifndef NDEBUG
                      << "\n Itemes received: " << _stat_items_received
                      << " pushed: " << _stat_items_pushed_to_target
            #endif
            << std::endl;
        #endif
    }
};
