#pragma once
#include <EdgeSwaps/EdgeSwapTFP.h>


namespace EdgeSwapTFP {
    struct LoadedEdgeSwapMsg {
        edge_t edge;
        swapid_t swap_id;

        LoadedEdgeSwapMsg() { }
        LoadedEdgeSwapMsg(const edge_t &edge_, const swapid_t &swap_id_) : edge(edge_), swap_id(swap_id_) {}

        DECL_LEX_COMPARE_OS(LoadedEdgeSwapMsg, edge, swap_id);
    };

    class SemiLoadedEdgeSwapTFP : public EdgeSwapTFP {
    public:
        using edge_update_sorter_t = EdgeUpdateSorter;
        using updated_edges_callback_t = std::function<void(EdgeUpdateSorter &)>;

    protected:
        using LoadedEdgeSwapComparator = GenericComparatorStruct<LoadedEdgeSwapMsg>::Ascending;
        using LoadedEdgeSwapSorter = stxxl::sorter<LoadedEdgeSwapMsg, LoadedEdgeSwapComparator>;
        std::unique_ptr<LoadedEdgeSwapSorter> _loaded_edge_swap_sorter;

        updated_edges_callback_t _updated_edges_callback;

        virtual void _start_processing(bool async = true);
        virtual void _process_swaps();

        template <class EdgeReader>
        void _compute_dependency_chain_semi_loaded(EdgeReader&, BoolStream&);
    public:
        SemiLoadedEdgeSwapTFP() = delete;
        SemiLoadedEdgeSwapTFP(const SemiLoadedEdgeSwapTFP &) = delete;

        //! Swaps are performed during constructor.
        //! @param edges  Edge vector changed in-place
        //! @param swaps  Read-only swap vector
        SemiLoadedEdgeSwapTFP( edge_buffer_t &edges, swapid_t run_length = 1000000) : 
            EdgeSwapTFP(edges, run_length),
            _loaded_edge_swap_sorter(new LoadedEdgeSwapSorter(LoadedEdgeSwapComparator(), _sorter_mem))
        {}

        // inherit the normal push method
        using EdgeSwapTFP::push;

        void push(const SemiLoadedSwapDescriptor &swap) {
           _loaded_edge_swap_sorter->push(LoadedEdgeSwapMsg(swap.edge(), _next_swap_id_pushing++));
           _edge_swap_sorter_pushing->push(EdgeSwapMsg(swap.eid(), _next_swap_id_pushing++));
           _swap_directions_pushing.push(swap.direction());

           if (UNLIKELY(_next_swap_id_pushing > 2*_run_length))
               _start_processing();
        };

        void setUpdatedEdgesCallback(updated_edges_callback_t callback) {
            _updated_edges_callback = callback;
        };

        void process_swaps() {
            _start_processing(false);
        };
    };

};
