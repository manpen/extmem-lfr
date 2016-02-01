#pragma once

//! @brief Drop-in replacement with identical interface to std::thread
//! which directly executes (and synchronously!) the main function
class SyncWorker {
public:
    using native_handle_type=void;

    //! The only meaningful member which directly executes the function
    //! with the argument provided.
    template <class Fn, class... Args>
    explicit SyncWorker (Fn&& fn, Args&&... args) {fn(args...);}

    SyncWorker() noexcept {}
    SyncWorker(const SyncWorker&) = delete;
    SyncWorker(SyncWorker&&) noexcept {}

    SyncWorker& operator= (SyncWorker&&) noexcept {return *this;}
    SyncWorker& operator= (const SyncWorker&) = delete;

    int get_id() const noexcept {return 0;}

    void join() {}
    bool joinable() {return false;}
    void detach() {}
    void swap(SyncWorker &) noexcept {}
    native_handle_type native_handle() {}
    static unsigned hardware_concurrency() noexcept {return 1;}
};
