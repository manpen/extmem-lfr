#include "EdgeSwapParallelTFP.h"

#include <algorithm>
#include <array>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>

#include <stx/btree_map>
#include <stxxl/priority_queue>

#include "PQSorterMerger.h"
#include "EdgeVectorUpdateStream.h"

namespace EdgeSwapParallelTFP {
    void EdgeSwapParallelTFP::process_swaps() {
        // if we have no swaps to load and no edges to write back, do nothing (might happen by calling process_swaps several times)
        if (_swap_id == 0 && _used_edge_ids.empty()) return;

        int old_num_threads = _num_threads;

        #pragma omp parallel num_threads(old_num_threads)
        #pragma omp single
        {
            _num_threads = omp_get_num_threads();
        }

        if (_num_threads < old_num_threads) {
            STXXL_ERRMSG("Warning: Did only get " << _num_threads << " threads in the parallel section instead of the requested " << old_num_threads << " threads.");
        }

        std::vector<std::unique_ptr<DependencyChainEdgeSorter>> swap_edge_sorter(_num_threads);
        std::vector<std::unique_ptr<DependencyChainSuccessorSorter>> swap_edge_dependencies_sorter(_num_threads);

        std::vector< std::unique_ptr< EdgeSwapParallelTFP::ExistenceInfoSorter > > existence_info_sorter(_num_threads);
        std::vector< std::unique_ptr< EdgeSwapParallelTFP::ExistenceSuccessorSorter > > existence_successor_sorter(_num_threads);
        std::vector< std::unique_ptr< EdgeSwapParallelTFP::ExistencePlaceholderSorter > > existence_placeholder_sorter(_num_threads);

        // allocate sorters only if there is actually something to do!
        if (_swap_id > 0) {
            // allocate sorters in parallel because of NUMA
            #pragma omp parallel num_threads(_num_threads)
            {
                auto tid = omp_get_thread_num();
                swap_edge_sorter[tid].reset(new DependencyChainEdgeSorter(DependencyChainEdgeComparatorSorter(), _sorter_mem/_num_threads));
                swap_edge_dependencies_sorter[tid].reset(new DependencyChainSuccessorSorter(DependencyChainSuccessorComparator(), _sorter_mem/_num_threads));

                existence_info_sorter[tid].reset(new ExistenceInfoSorter(ExistenceInfoComparator(), _sorter_mem/_num_threads));
                existence_successor_sorter[tid].reset(new ExistenceSuccessorSorter(ExistenceSuccessorComparator(), _sorter_mem/_num_threads));
                existence_placeholder_sorter[tid].reset(new ExistencePlaceholderSorter(ExistencePlaceholderComparator(), _sorter_mem/_num_threads));
            }
        }

        _load_and_update_edges(swap_edge_sorter, swap_edge_dependencies_sorter);

        if (_swap_id > 0) {
            for (auto &w : _swap_direction_writer) {
                w->finish();
            }

            {
                ExistenceRequestMerger existence_merger(ExistenceRequestComparator(), SORTER_MEM);

                _compute_conflicts(swap_edge_sorter, swap_edge_dependencies_sorter, existence_merger);
                _process_existence_requests(existence_merger, existence_info_sorter, existence_successor_sorter, existence_placeholder_sorter);
            }

            _perform_swaps(swap_edge_sorter, swap_edge_dependencies_sorter, existence_info_sorter, existence_successor_sorter, existence_placeholder_sorter);

            for (int i = 0; i < _num_threads; ++i) {
                _swap_direction_writer[i].reset(new BoolVector::bufwriter_type(*_swap_direction[i]));
            }
        }

        // re-initialize data structures for new swaps
        _swap_id = 0;
        _edge_swap_sorter.clear();
        _num_threads = old_num_threads;

    }

    void EdgeSwapParallelTFP::_load_and_update_edges(std::vector<std::unique_ptr<DependencyChainEdgeSorter>> &edge_output, std::vector<std::unique_ptr<DependencyChainSuccessorSorter>> &dependency_output) {
        uint64_t numSwaps = _swap_id;

        // copy old edge ids for writing back
        EdgeIdVector old_edge_ids;
        old_edge_ids.swap(_used_edge_ids);
        _used_edge_ids.reserve(_swap_id * 2);
        EdgeIdVector::bufwriter_type edge_id_writer(_used_edge_ids);

        _edge_swap_sorter.sort();


        if (compute_stats) {
            std::cout << "Requesting " << _edge_swap_sorter.size() << " non-unique edges for internal swaps" << std::endl;
        }


        { // load edges from EM. Generates successor information and swap_edges information (for the first edge in the chain).
            auto use_edge = [&] (const edge_t & cur_e, edgeid_t id) {
                swapid_t sid;
                unsigned char spos;
                int tid;

                auto match_request = [&]() {
                    if (!_edge_swap_sorter.empty() && _edge_swap_sorter->eid == id) {
                        sid = _edge_swap_sorter->sid;
                        spos = _edge_swap_sorter->spos;
                        tid = _thread(sid);
                        assert(tid < _num_threads);
                        ++_edge_swap_sorter;
                        return true;
                    } else {
                        return false;
                    }
                };

                if (match_request()) {
                    assert(edge_output[tid]);
                    assert(dependency_output[tid]);
                    edge_id_writer << id;
                    edge_output[tid]->push(DependencyChainEdgeMsg {sid, spos, cur_e});

                    auto lastSpos = spos;
                    auto lastSid = sid;
                    auto lastTid = tid;


                    // further requests for the same swap - store successor information
                    while (match_request()) {
                        // set edge id to internal edge id
                        assert(dependency_output[lastTid]);
                        dependency_output[lastTid]->push(DependencyChainSuccessorMsg {lastSid, lastSpos, sid, spos});
                        lastSpos = spos;
                        lastSid = sid;
                        lastTid = tid;
                    }
                }
            };

            edgeid_t id = 0;
            typename edge_vector::bufreader_type edge_reader(_edges);

            if (old_edge_ids.empty()) {
                // just read edges
                for (; !edge_reader.empty(); ++id, ++edge_reader) {
                    use_edge(*edge_reader, id);
                }
            } else {
                // read old edge vector and merge in updates, write out result
                edge_vector output_vector;
                output_vector.reserve(_edges.size());
                typename edge_vector::bufwriter_type writer(output_vector);

                EdgeIdVector::bufreader_type old_e(old_edge_ids);
                int_t read_id = 0;
                edge_t cur_e;

                for (; !edge_reader.empty() || !_edge_update_merger.empty(); ++id) {
                    // Skip old edges
                    while (!old_e.empty() && *old_e == read_id) {
                        ++edge_reader;
                        ++read_id;
                        ++old_e;
                    }

                    // merge update edges and read edges
                    if (!_edge_update_merger.empty() && (edge_reader.empty() || *_edge_update_merger < *edge_reader)) {
                        cur_e = *_edge_update_merger;
                        writer << cur_e;
                        ++_edge_update_merger;
                    } else {
                        if (edge_reader.empty()) { // due to the previous while loop both could be empty now
                            break; // abort the loop as we do not have any edges to process anymore.
                        }

                        cur_e = *edge_reader;
                        writer << cur_e;
                        ++read_id;
                        ++edge_reader;
                    }

                    use_edge(cur_e, id);
                }

                writer.finish();
                _edges.swap(output_vector);

                _edge_update_merger.deallocate();
            }
        }


        if (numSwaps > 0) {
            #pragma omp parallel num_threads(_num_threads)
            {
                auto tid = omp_get_thread_num();
                edge_output[tid]->sort();
                dependency_output[tid]->sort();
            }

            _edge_swap_sorter.finish_clear();
        }
    }

    /*
     * Since we do not yet know whether an swap can be performed, we keep for
     * every edge id a set of possible states. Initially this state is only the
     * edge as fetched in _compute_dependency_chain(), but after the first swap
     * the set contains at least two configurations, i.e. the original state
     * (in case the swap cannot be performed) and the swapped state.
     *
     * These configurations are kept in depchain_edge_pq: Each swap receives
     * the complete state set of both edges and computes the cartesian product
     * of both. If there exists a successor swap (info stored in
     * _depchain_successor_sorter), i.e. a swap that will be  affect by the
     * current one, these information are forwarded.
     *
     * We further request information whether the edge exists by pushing requests
     * into _existence_request_sorter.
     */
    void EdgeSwapParallelTFP::_compute_conflicts(std::vector< std::unique_ptr< EdgeSwapParallelTFP::DependencyChainEdgeSorter > > &swap_edges, std::vector< std::unique_ptr< EdgeSwapParallelTFP::DependencyChainSuccessorSorter > > &dependencies, ExistenceRequestMerger &requestOutputMerger) {
        using DependencyChainEdgeComparatorPQ = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Descending;

        // FIXME make sure that this leads to useful sort buffer sizes!
        const auto existence_request_buffer_size = SORTER_MEM/_num_threads/sizeof(ExistenceRequestMsg);
        swapid_t batch_size_per_thread = 1*IntScale::Mi;

        struct edge_information_t {
            std::array<int_t, 2> is_set;
            std::array<std::vector<edge_t>, 2> edges;
        };

        // pointers are used to make sure that everything is in the memory region of the specific thread
        std::vector<std::unique_ptr<std::vector<edge_information_t>>> edge_information(_num_threads);

        stxxl::stream::runs_creator<stxxl::stream::from_sorted_sequences<ExistenceRequestMsg>,
        ExistenceRequestComparator, STXXL_DEFAULT_BLOCK_SIZE(ExistenceRequestMsg), STXXL_DEFAULT_ALLOC_STRATEGY> existence_request_runs_creator (ExistenceRequestComparator(), SORTER_MEM);

        RunsCreatorThread<decltype(existence_request_runs_creator)> existence_request_runs_creator_thread(existence_request_runs_creator);

        using DependencyChainParallelPQSorterMerger = ParallelBufferedPQSorterMerger<DependencyChainEdgeSorter, DependencyChainEdgeComparatorPQ>;
        DependencyChainParallelPQSorterMerger global_parallel_pq_sorter_merger(_num_threads, swap_edges);

        #pragma omp parallel num_threads(_num_threads)
        {

            int tid = omp_get_thread_num();

            edge_information[tid].reset(new std::vector<edge_information_t>(batch_size_per_thread));
            auto &my_edge_information = *edge_information[tid];

            DependencyChainParallelPQSorterMerger::ThreadData depchain_pqsort(global_parallel_pq_sorter_merger, tid);

            BoolVector::bufreader_type direction_reader(*_swap_direction[tid]);

            auto &dep = *dependencies[tid];

            RunsCreatorBuffer<decltype(existence_request_runs_creator)> existence_request_buffer(existence_request_runs_creator_thread, existence_request_buffer_size);

            swapid_t loop_limit = _swap_id;
            {
                swapid_t remainder = _swap_id % _num_threads;
                if (remainder != 0) loop_limit += (_num_threads - remainder);
            }
            swapid_t sid = tid;
            while  (sid < loop_limit) { // execution of batch starts
                swapid_t sid_in_batch_base = sid-tid;
                swapid_t sid_in_batch_limit = std::min<swapid_t>(_swap_id, sid_in_batch_base + batch_size_per_thread * _num_threads);

                for (swapid_t i = 0; i < batch_size_per_thread && sid < loop_limit; ++i, sid += _num_threads) {
                    if (UNLIKELY(sid >= _swap_id)) continue;
                    std::array<std::vector<edge_t>, 2> current_edges;
                    std::array<std::vector<edge_t>, 2> dd_new_edges;

                    std::array<swapid_t, 2> successor_sid;
                    std::array<unsigned char, 2> successor_spos;


                    assert(!direction_reader.empty());

                    bool direction = *direction_reader;
                    ++direction_reader;

                    // fetch messages sent to this edge
                    for (unsigned int spos = 0; spos < 2; spos++) {
                        // get successor
                        if (!dep.empty()) {
                            auto &msg = *dep;

                            assert(msg.swap_id >= sid);
                            assert(msg.swap_id > sid || msg.spos >= spos);

                            if (msg.swap_id != sid || msg.spos != spos) {
                                successor_sid[spos] = 0;
                            } else {
                                DEBUG_MSG(_display_debug, "Got successor for S" << sid << ", E" << spos << ": " << msg);
                                successor_sid[spos] = msg.successor;
                                successor_spos[spos] = msg.successor_spos;
                                ++dep;
                            }
                        } else {
                            successor_sid[spos] = 0;
                        }


                        // fetch possible edge state before swap
                        while (!my_edge_information[i].is_set[spos] && !depchain_pqsort.empty()) {
                            const auto & msg = *depchain_pqsort;

                            if (msg.swap_id != sid || msg.spos != spos)
                                break;

                            current_edges[spos].push_back(msg.edge);

                            /* // this optimization isn't possible in the parallel case as otherwise we cannot find out anymore if we got a message or not.
                            // ensure that the first entry in edges is from sorter, so we do not have to resent it using PQ
                            if (UNLIKELY(depchain_pqsort.source() == SrcSorter && edges[i].size() != 1)) {
                                std::swap(edges[i].front(), edges[i].back());
                            }
                            */

                            ++depchain_pqsort;
                        }

                        // ensure that we received at least one state of the edge before the swap
                        if (current_edges[spos].empty()) {
                            // TODO: find out if this is okay or if we should rather use some condition variable here in order to give the background output threads more cpu time (or wait e.g. 2ms)
                            while (!my_edge_information[i].is_set[spos]) { // busy waiting for other thread to supply information
                                std::this_thread::yield();
                            }

                            current_edges[spos] = std::move(my_edge_information[i].edges[spos]); // FIXME this move is bad as the memory might be in another memory region.
                            // TODO: copy instead of moving but make sure that the memory in the in my_edge_information is still freed
                            my_edge_information[i].is_set[spos] = false;
                        }

                        DEBUG_MSG(_display_debug, "SWAP " << sid << " Edge " << spos << " Successor: " << successor_sid[spos] << " States: " << current_edges[spos].size());

                        assert(!current_edges[spos].empty());

                        // ensure that dependent swap is in fact a successor (i.e. has larger index)
                        assert(successor_sid[spos] == 0 || successor_sid[spos] > sid);
                    }

                    // ensure that all messages to this swap have been consumed
                    assert(depchain_pqsort.empty() || (*depchain_pqsort).swap_id > sid);


                    #ifndef NDEBUG
                        if (_display_debug) {
                            std::cout << "Swap " << sid << " edges[0] = [";
                            for (auto &e : current_edges[0]) std::cout << e << " ";
                            std::cout << "] edges[1]= [";
                            for (auto &e : current_edges[1]) std::cout << e << " ";
                            std::cout << "]" << std::endl;
                        }
                    #endif

                    // compute "cartesian" product between possible edges to determine all possible new edges
                    dd_new_edges[0].clear();
                    dd_new_edges[1].clear();

                    for (auto &e1 : current_edges[0]) {
                        for (auto &e2 : current_edges[1]) {
                            edge_t new_edges[2];
                            std::tie(new_edges[0], new_edges[1]) = _swap_edges(e1, e2, direction);

                            for (unsigned int i = 0; i < 2; i++) {
                                auto &new_edge = new_edges[i];
                                auto &queue = dd_new_edges[i];

                                queue.push_back(new_edge);

                                DEBUG_MSG(_display_debug, "Swap " << sid << " may yield " << new_edge << " at " << i);
                            }
                        }
                    }

                    for (unsigned char spos = 0; spos < 2; spos++) {
                        auto & dd = dd_new_edges[spos];

                        // sort to support binary search and linear time deduplication
                        if (dd.size() > 1) {
                            std::sort(dd.begin(), dd.end());
                            // TODO move this in merge (see fixme below)
                            auto last = std::unique(dd.begin(), dd.end()); // eliminate duplicates
                            dd.erase(last, dd.end());
                        }

                        bool has_successor_in_batch = false;
                        bool has_successor_in_other_batch = false;
                        int_t successor_tid = 0;
                        if (successor_sid[spos]) {
                            successor_tid = _thread(successor_sid[spos]);
                            if (successor_sid[spos] < sid_in_batch_limit) {
                                has_successor_in_batch = true;
                            } else {
                                has_successor_in_other_batch = true;
                            }
                        }

                        for (const auto &e : dd) {
                            existence_request_buffer.push(ExistenceRequestMsg {e, sid, false});
                            if (UNLIKELY(has_successor_in_other_batch)) {
                                depchain_pqsort.push(DependencyChainEdgeMsg {successor_sid[spos], successor_spos[spos], e}, successor_tid);
                            }
                        }

                        for (const auto &e : current_edges[spos]) {
                            // check whether already sent above
                            // FIXME use combined iteration over dd and current_edges instead with merging!
                            if (UNLIKELY(std::binary_search(dd.cbegin(), dd.cend(), e)))
                                continue;

                            existence_request_buffer.push(ExistenceRequestMsg{e, sid, true});
                            if (UNLIKELY(has_successor_in_other_batch)) {
                                depchain_pqsort.push(DependencyChainEdgeMsg {successor_sid[spos], successor_spos[spos], e}, successor_tid);
                            }
                        }

                        if (has_successor_in_batch) {
                            auto pos = (successor_sid[spos] - sid_in_batch_base)/_num_threads;
                            auto & t_edges = (*edge_information[successor_tid])[pos].edges[successor_spos[spos]];

                            t_edges.clear();
                            t_edges.reserve(current_edges[spos].size() + dd.size());

                            std::set_union(current_edges[spos].begin(), current_edges[spos].end(),
                                dd.begin(), dd.end(),
                                std::back_inserter(t_edges));

                            #pragma omp atomic write seq_cst // make sure that the vector is flushed before is_set is updated!
                            (*edge_information[successor_tid])[pos].is_set[successor_spos[spos]] = 1;
                        }
                    }
                }

                // finished batch.

                { // sort buffer and enqueue sorted buffer to be written out TODO check if this should be after the PQ population
                    existence_request_buffer.finish();
                }

                #pragma omp barrier // TODO calculate wait time at this barrier as this is the first barrier after the processing of all swaps!

                // flush the buffers in the pq. note that during the flush no pushes must happen, therefore we need a barrier before and after the flush.
                depchain_pqsort.flush_buffer();

                #pragma omp barrier
            } // finished processing all swaps of the current run

            existence_request_buffer.flush(); // make sure all requests are processed!

            swap_edges[tid]->rewind();
            dependencies[tid]->rewind();
        } // end of parallel section

        requestOutputMerger.initialize(existence_request_runs_creator.result());
    }

    /*
     * We parallel stream through _edges and requestMerger
     * to check whether a requested edge exists in the input graph.
     * The result is sent to the first swap requesting using
     * existence_info_output. We additionally compute a dependency chain
     * by informing every swap about the next one requesting the info and inform each swap how many edges it will get using placeholders.
     */
    void EdgeSwapParallelTFP::_process_existence_requests(ExistenceRequestMerger &requestMerger,
        std::vector< std::unique_ptr< EdgeSwapParallelTFP::ExistenceInfoSorter > > &existence_info_output,
        std::vector< std::unique_ptr< EdgeSwapParallelTFP::ExistenceSuccessorSorter > > &successor_output,
        std::vector< std::unique_ptr< EdgeSwapParallelTFP::ExistencePlaceholderSorter > > &existence_placeholder_output) {

        typename edge_vector::bufreader_type edge_reader(_edges);

        while (!requestMerger.empty()) {
            auto &request = *requestMerger;
            edge_t current_edge = request.edge;

            // find edge in graph
            bool exists = false;
            for (; !edge_reader.empty(); ++edge_reader) {
                const auto &edge = *edge_reader;
                if (edge > current_edge) break;
                exists = (edge == current_edge);
            }

            // build depencency chain (i.e. inform earlier swaps about later ones) and find the earliest swap
            swapid_t last_swap = request.swap_id;
            bool foundTargetEdge = false; // if we already found a swap where the edge is a target
            for (; !requestMerger.empty(); ++requestMerger) {
                auto &request = *requestMerger;
                if (request.edge != current_edge)
                    break;

                if (last_swap != request.swap_id && foundTargetEdge) {
                    // inform an earlier swap about later swaps that need the new state
                    assert(last_swap > request.swap_id);
                    successor_output[_thread(request.swap_id)]->push(ExistenceSuccessorMsg{request.swap_id, current_edge, last_swap});
                    existence_placeholder_output[_thread(last_swap)]->push(last_swap);
                    DEBUG_MSG(_display_debug, "Inform swap " << request.swap_id << " that " << last_swap << " is a successor for edge " << current_edge);
                }

                last_swap = request.swap_id;
                foundTargetEdge = (foundTargetEdge || !request.forward_only);
            }

            // inform earliest swap whether edge exists
            if (foundTargetEdge && exists) {
                auto tid = _thread(last_swap);
                existence_info_output[tid]->push(ExistenceInfoMsg{last_swap, current_edge});
                existence_placeholder_output[tid]->push(last_swap);
                DEBUG_MSG(_display_debug, "Inform swap " << last_swap << " edge " << current_edge << " exists " << exists);
            }
        }

        #pragma omp parallel num_threads(_num_threads)
        {
            auto tid = omp_get_thread_num();
            existence_info_output[tid]->sort();
            existence_placeholder_output[tid]->sort();
            successor_output[tid]->sort();
        }
    }

    /*
     * Information sources:
     *  _swaps contains definition of swaps
     *  _depchain_successor_sorter stores swaps we need to inform about our actions
     */
    void EdgeSwapParallelTFP::_perform_swaps(std::vector< std::unique_ptr< EdgeSwapParallelTFP::DependencyChainEdgeSorter > > &edges_sorter,
        std::vector< std::unique_ptr< EdgeSwapParallelTFP::DependencyChainSuccessorSorter > > &edge_dependencies,
        std::vector< std::unique_ptr< EdgeSwapParallelTFP::ExistenceInfoSorter > > &existence_info_sorter,
        std::vector< std::unique_ptr< EdgeSwapParallelTFP::ExistenceSuccessorSorter > > &existence_successor,
        std::vector< std::unique_ptr< EdgeSwapParallelTFP::ExistencePlaceholderSorter > > &existence_placeholder) {

        // FIXME make sure that this leads to useful sort buffer sizes!
        swapid_t batch_size_per_thread = 1*IntScale::Mi;

#ifdef EDGE_SWAP_DEBUG_VECTOR
        // debug only
        // this is not good for NUMA, but hey, this is debug mode (+ this is write once + read once in a single thread, so either writing or reading is bad anyway)
        std::vector<std::vector<debug_vector::value_type>> debug_output_buffer(_num_threads);
        for (auto & v : debug_output_buffer) {
            v.reserve(batch_size_per_thread);
        }
#endif

        stxxl::stream::runs_creator<stxxl::stream::from_sorted_sequences<edge_t>,
        EdgeUpdateComparator, STXXL_DEFAULT_BLOCK_SIZE(edge_t), STXXL_DEFAULT_ALLOC_STRATEGY> edge_update_runs_creator (EdgeUpdateComparator(), SORTER_MEM);

        RunsCreatorThread<decltype(edge_update_runs_creator)> edge_update_runs_creator_thread(edge_update_runs_creator);

        // we need to use a desc-comparator since the pq puts the largest element on top
        using ExistenceInfoComparatorPQ = typename GenericComparatorStruct<ExistenceInfoMsg>::Descending;
        using ExistenceInfoParallelPQSorterMerger = ParallelBufferedPQSorterMerger<ExistenceInfoSorter, ExistenceInfoComparatorPQ>;
        ExistenceInfoParallelPQSorterMerger global_existence_info_pq_sorter_merger(_num_threads, existence_info_sorter);

        using DependencyChainEdgeComparatorPQ = typename GenericComparatorStruct<DependencyChainEdgeMsg>::Descending;
        using DependencyChainParallelPQSorterMerger = ParallelBufferedPQSorterMerger<DependencyChainEdgeSorter, DependencyChainEdgeComparatorPQ>;
        DependencyChainParallelPQSorterMerger global_dependency_chain_pq_sorter_merger(_num_threads, edges_sorter);

        std::vector<std::unique_ptr<std::vector<std::array<edge_t, 2>>>> source_edges(_num_threads);

        struct edge_existence_info_t {
            std::vector<edge_t> edges;
            size_t size;
        };

        std::vector<std::unique_ptr<std::vector<edge_existence_info_t>>> existence_information(_num_threads);


        #pragma omp parallel num_threads(_num_threads)
        {
            const auto tid = omp_get_thread_num();

            source_edges[tid].reset(new std::vector<std::array<edge_t, 2>>(batch_size_per_thread, std::array<edge_t, 2>{edge_t{-1, -1}, edge_t{-1, -1}}));
            auto &my_source_edges = *source_edges[tid];

            existence_information[tid].reset(new std::vector<edge_existence_info_t>(batch_size_per_thread));
            auto &my_existence_information = *existence_information[tid];

            auto &my_edge_dependencies = *edge_dependencies[tid];

            auto &my_existence_sucessors = *existence_successor[tid];

            ExistenceInfoParallelPQSorterMerger::ThreadData existence_info_pqsort(global_existence_info_pq_sorter_merger, tid);
            DependencyChainParallelPQSorterMerger::ThreadData edge_state_pqsort(global_dependency_chain_pq_sorter_merger, tid);

            RunsCreatorBuffer<decltype(edge_update_runs_creator)> edge_update_buffer(edge_update_runs_creator_thread, batch_size_per_thread * 2);

            BoolVector::bufreader_type direction_reader(*_swap_direction[tid]);

            swapid_t sid = tid;

            swapid_t loop_limit = _swap_id;
            {
                swapid_t remainder = _swap_id % _num_threads;
                if (remainder != 0) loop_limit += (_num_threads - remainder);
            }
            while  (sid < loop_limit) { // execution of batch starts
                swapid_t sid_in_batch_base = sid-tid;
                swapid_t sid_in_batch_limit = std::min<swapid_t>(_swap_id, sid_in_batch_base + batch_size_per_thread * _num_threads);

                auto &my_existence_placeholder = *existence_placeholder[tid];
                for (swapid_t s = sid, i = 0; i < batch_size_per_thread && s < _swap_id; ++i, s += _num_threads) {
                    size_t c = 0;
                    while (!my_existence_placeholder.empty() && *my_existence_placeholder == s) {
                            ++c;
                        ++my_existence_placeholder;
                    }

                    my_existence_information[i].size = 0;
                    my_existence_information[i].edges.resize(c);
                }

                #pragma omp barrier

                for (swapid_t i = 0; i < batch_size_per_thread && sid < loop_limit; ++i, sid += _num_threads) {
                    if (UNLIKELY(sid >= _swap_id)) continue;
                    auto & ex_info = my_existence_information[i];
                    auto & cur_edges = my_source_edges[i];

                    std::array<edge_t, 2> new_edges;

                    assert(!direction_reader.empty());

                    bool direction = *direction_reader;
                    ++direction_reader;


                    for (unsigned int spos = 0; spos < 2; spos++) {
                        // get edge successors
                        // fetch possible edge state before swap
                        while (!edge_state_pqsort.empty()) {
                            const auto & msg = *edge_state_pqsort;

                            if (msg.swap_id != sid || msg.spos != spos)
                                break;

                            cur_edges[spos] = msg.edge;

                            ++edge_state_pqsort;
                        }

                        // possibly wait for another thread to set the information
                        if (cur_edges[spos] == edge_t{-1, -1}) {
                            // TODO: find out if this is okay or if we should rather use some condition variable here in order to give the background output threads more cpu time (or wait e.g. 2ms)
                            while (cur_edges[spos] == edge_t{-1, -1}) { // busy waiting for other thread to supply information
                                std::this_thread::yield();
                            }
                        }
                    }

                    // compute swapped edges
                    std::tie(new_edges[0], new_edges[1]) = _swap_edges(cur_edges[0], cur_edges[1], direction);

                    #ifndef NDEBUG
                        if (_display_debug) {
                            std::cout << "State in " << sid << ": ";
                            std::cout << cur_edges[0] << ", " << cur_edges[1] << " ";
                            std::cout << new_edges[0] << ", " << new_edges[1] <<  " ";
                            std::cout << std::endl;
                        }
                    #endif

                    // gather all edge states that have been sent to this swap
                    {
                        for (; !existence_info_pqsort.empty() && (*existence_info_pqsort).swap_id == sid; ++existence_info_pqsort) {
                            const auto &msg = *existence_info_pqsort;

                            size_t k;

                            #pragma omp atomic capture
                            k = ex_info.size++;

                            assert(k < ex_info.edges.size());

                            ex_info.edges[k] = msg.edge;
                        }

                        while (ex_info.size < ex_info.edges.size()) { // busy waiting for other thread to supply information
                            std::this_thread::yield();
                        }

                        #ifndef NDEBUG
                            if (_display_debug) {
                                for (auto &k : ex_info.edges)
                                    std::cout << sid << " " << k << " exists" << std::endl;
                            }
                        #endif
                    }

                    // check if there's a conflicting edge
                    bool conflict_exists[2];
                    for (unsigned int i = 0; i < 2; i++) {
                        // check if linear search is okay here or if we need to sort the existence info
                        bool exists = (std::find(ex_info.edges.begin(), ex_info.edges.end(), new_edges[i]) != ex_info.edges.end());
                        conflict_exists[i] = exists;
                    }

                    // can we perform the swap?
                    const bool loop = new_edges[0].is_loop() || new_edges[1].is_loop();
                    const bool perform_swap = !(conflict_exists[0] || conflict_exists[1] || loop);


#ifdef EDGE_SWAP_DEBUG_VECTOR
                    // write out debug message
                    {
                        SwapResult res;
                        res.performed = perform_swap;
                        res.loop = loop;
                        for(unsigned int spos=0; spos < 2; spos++) {
                            res.edges[spos] = new_edges[spos];
                            res.conflictDetected[spos] = conflict_exists[spos];
                        }
                        res.normalize();

                        debug_output_buffer[tid].push_back(res);
                        DEBUG_MSG(_display_debug, "Swap " << sid << " " << res);
                    }

                    if (!perform_swap) {
                        new_edges[0] = cur_edges[0];
                        new_edges[1] = cur_edges[1];
                    }
#endif

                    // forward edge state to successor swap
                    std::array<bool, 2> successor_found = {false, false};
                    while (!my_edge_dependencies.empty() && my_edge_dependencies->swap_id == sid) {
                        auto &msg = *my_edge_dependencies;
                        DEBUG_MSG(_display_debug, "Got successor for S" << sid << ", E" << msg.spos << ": " << msg);

                        successor_found[msg.spos] = true;

                        auto successor_tid = _thread(msg.successor);
                        if (msg.successor < sid_in_batch_limit) {
                            auto pos = (msg.successor - sid_in_batch_base)/_num_threads;
                            (*source_edges[successor_tid])[pos][msg.successor_spos] = new_edges[msg.spos];
                        } else {
                            edge_state_pqsort.push(DependencyChainEdgeMsg {msg.successor, msg.successor_spos, new_edges[msg.spos]}, successor_tid);
                        }

                        ++my_edge_dependencies;
                    }

                    // send current state of edge iff there are no successors to this edge
                    for(unsigned int spos=0; spos <2; spos++) {
                        if (!successor_found[spos]) {
                            edge_update_buffer.push(new_edges[spos]);
                        }
                    }

                    auto push_existence_info = [&](swapid_t target_sid, edge_t e, bool exists) {
                        auto successor_tid = _thread(target_sid);

                        // if the edge does not exist send invalid edge so it won't find it (but still gets enough messages)
                        if (!exists) e = edge_t{-1, -1};

                        if (target_sid < sid_in_batch_limit) {
                            auto pos = (target_sid - sid_in_batch_base)/_num_threads;
                            edge_existence_info_t &e_in = (*existence_information[successor_tid])[pos];

                            size_t s;
                            #pragma omp atomic capture
                            s = e_in.size++;

                            e_in.edges[s] = e;
                        } else {
                            existence_info_pqsort.push(ExistenceInfoMsg{target_sid, e}, successor_tid);
                        }
                    };

                    // forward existence information
                    for (; !my_existence_sucessors.empty(); ++my_existence_sucessors) {
                        auto &succ = *my_existence_sucessors;

                        assert(succ.swap_id >= sid);
                        if (succ.swap_id > sid) break;

                        if (succ.edge == new_edges[0] || succ.edge == new_edges[1]) {
                            // target edges always exist (or source if no swap has been performed)
                            push_existence_info(succ.successor, succ.edge, true);
                            DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << true << " to " << succ.successor);
                        } else if (succ.edge == cur_edges[0] || succ.edge == cur_edges[1]) {
                            // source edges never exist (if no swap has been performed, this has been handled above)
                            push_existence_info(succ.successor, succ.edge, false);
                            DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << false << " to " << succ.successor);
                        } else {
                            bool exists = (std::find(ex_info.edges.begin(), ex_info.edges.end(), succ.edge) != ex_info.edges.end());
                            push_existence_info(succ.successor, succ.edge, exists);
                            DEBUG_MSG(_display_debug, "Send " << succ.edge << " exists: " << exists << " to " << succ.successor);
                        }
                    }

                    cur_edges[0] = {-1, -1};
                    cur_edges[1] = {-1, -1};
                }
                // finished batch

                edge_update_buffer.finish();

                #pragma omp barrier

#ifdef EDGE_SWAP_DEBUG_VECTOR
                #pragma omp single
                {
                    for (swapid_t i = 0, s = 0; i < batch_size_per_thread && s < _swap_id; ++i) {
                        for (int tid = 0; tid < _num_threads && s < _swap_id; ++tid, ++s) {
                            _debug_vector_writer << debug_output_buffer[tid][i];
                        }
                    }
                }
#endif

                edge_state_pqsort.flush_buffer();
                existence_info_pqsort.flush_buffer();

                #pragma omp barrier

            }

            // check message data structures are empty
            assert(my_edge_dependencies.empty());
            //_depchain_successor_sorter.finish_clear();

            assert(my_existence_sucessors.empty());
            //_existence_successor_sorter.finish_clear();

            assert(existence_info_pqsort.empty());
            //_existence_info_sorter.finish_clear();

            edge_update_buffer.flush();
        }// end of parallel region


        _edge_update_merger.initialize(edge_update_runs_creator.result());
    }
};
