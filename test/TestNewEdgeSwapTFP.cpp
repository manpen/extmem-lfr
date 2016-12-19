#include <gtest/gtest.h>

#include <stxxl/vector>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <EdgeSwaps/EdgeSwapTFP.h>
#include <EdgeSwaps/EdgeSwapParallelTFP.h>
#include <EdgeSwaps/IMEdgeSwap.h>
#include <ConfigurationModel.h>
#include <Utils/StreamPusher.h>
#include <EdgeSwaps/MultiEdgeSwapFullyInternal.h>
#include <DegreeDistributionCheck.h>

namespace {
	using EdgeVector = stxxl::vector<edge_t>;
	using SwapVector = stxxl::vector<SwapDescriptor>;
	using AlgoFullyInternal = MultiEdgeSwapFullyInternal<>;

	template <class Algo>
	class TestNewEdgeSwapTFP : public ::testing::Test {
		protected:
	      template <class List>
	      void _print_list(List & list, bool show = true) {
	         if (!show) return;
	         unsigned int i = 0;
	         for(auto &e : list)
	            std::cout << i++ << " " << e << std::endl;
	      }

	      void _list_to_stream(EdgeVector & in, EdgeStream & out) {
	        for(auto & e : in) {
	            out.push(e);
	        }
	        out.consume();
	      }

	       void _stream_to_list(EdgeStream & in, EdgeVector & out) {
	          out.clear();
	          for(; !in.empty(); ++in)
	             out.push_back(*in);
	       }
	};

	using TestNewEdgeSwapTFPImplementations = ::testing::Types <
	  // EdgeSwapFullyInternal<EdgeVector, SwapVector>,

      //EdgeSwapInternalSwaps,
      EdgeSwapTFP::EdgeSwapTFP, IMEdgeSwap
      //EdgeSwapParallelTFP::EdgeSwapParallelTFP,
   	>;

    TYPED_TEST_CASE(TestNewEdgeSwapTFP, TestNewEdgeSwapTFPImplementations);

   	TYPED_TEST(TestNewEdgeSwapTFP, noConflicts) {
   		bool debug_this_test = true;
   		using EdgeSwapAlgo = TypeParam;

   		EdgeVector edge_list;
   		EdgeStream edge_stream(true, true);
   		edge_list.push_back({1, 3});
   		edge_list.push_back({2, 4});
   		edge_list.push_back({2, 4});
   		edge_list.push_back({3, 3});
   		edge_list.push_back({3, 6});
   		edge_list.push_back({5, 6});

      	this->_list_to_stream(edge_list, edge_stream);

   		SwapVector swap_list;
   		swap_list.push_back({0, 1, true});
   		swap_list.push_back({1, 2, false});
   		swap_list.push_back({3, 5, true});

	    EdgeSwapAlgo algo(edge_stream, swap_list);

	    algo.setDisplayDebug(debug_this_test);

		if (EdgeSwapTrait<EdgeSwapAlgo>::pushableSwaps()) {
	        for (auto &s : swap_list)
	            algo.push(s);
      	}

	    algo.run();
	    this->_stream_to_list(edge_stream, edge_list);


	    this->_print_list(edge_list, debug_this_test);

	    ASSERT_EQ(edge_list[0], edge_t(1, 4));
	    ASSERT_EQ(edge_list[1], edge_t(2, 3));
	    ASSERT_EQ(edge_list[2], edge_t(2, 4));
	    ASSERT_EQ(edge_list[3], edge_t(3, 3));
	    ASSERT_EQ(edge_list[4], edge_t(3, 6));
	    ASSERT_EQ(edge_list[5], edge_t(5, 6));

   	}

   	TYPED_TEST(TestNewEdgeSwapTFP, test2) {
   		bool debug_this_test = true;
   		using EdgeSwapAlgo = TypeParam;

   		EdgeVector edge_list;
   		EdgeStream edge_stream(true, true);
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 9});
   		edge_list.push_back({2, 10});
   		edge_list.push_back({3, 4});
   		edge_list.push_back({5, 6});
   		edge_list.push_back({7, 8});

      	this->_list_to_stream(edge_list, edge_stream);

   		SwapVector swap_list;
   		swap_list.push_back({3, 4, false});
   		swap_list.push_back({0, 5, false});
   		swap_list.push_back({3, 4, false});
   		swap_list.push_back({1, 6, false});
   		swap_list.push_back({3, 4, false});
   		swap_list.push_back({2, 7, false});
   		swap_list.push_back({3, 4, false});
		//swap_list.push_back({3, 4, false});

	    EdgeSwapAlgo algo(edge_stream, swap_list);
	    algo.setDisplayDebug(debug_this_test);

		if (EdgeSwapTrait<EdgeSwapAlgo>::pushableSwaps()) {
	        for (auto &s : swap_list)
	            algo.push(s);
      	}

	    algo.run();
	    this->_stream_to_list(edge_stream, edge_list);


	    this->_print_list(edge_list, debug_this_test);

	    ASSERT_EQ(edge_list[0], edge_t(1, 2));
	    ASSERT_EQ(edge_list[1], edge_t(1, 3));
	    ASSERT_EQ(edge_list[2], edge_t(1, 5));
	    ASSERT_EQ(edge_list[3], edge_t(1, 7));
	    ASSERT_EQ(edge_list[4], edge_t(2, 4));
	    ASSERT_EQ(edge_list[5], edge_t(2, 6));
	    ASSERT_EQ(edge_list[6], edge_t(2, 8));
	    ASSERT_EQ(edge_list[7], edge_t(9, 10));

   	}

   	TYPED_TEST(TestNewEdgeSwapTFP, deletion) {
   		bool debug_this_test = true;
   		using EdgeSwapAlgo = TypeParam;

   		EdgeVector edge_list;
   		EdgeStream edge_stream(true, true);
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 2});
   		edge_list.push_back({1, 3});
   		edge_list.push_back({2, 4});
   		edge_list.push_back({5, 6});

      	this->_list_to_stream(edge_list, edge_stream);

   		SwapVector swap_list;
   		swap_list.push_back({0, 5, true});
   		swap_list.push_back({3, 4, false});

	    EdgeSwapAlgo algo(edge_stream, swap_list);
	    algo.setDisplayDebug(debug_this_test);

		if (EdgeSwapTrait<EdgeSwapAlgo>::pushableSwaps()) {
	        for (auto &s : swap_list)
	            algo.push(s);
      	}

	    algo.run();
	    this->_stream_to_list(edge_stream, edge_list);


	    this->_print_list(edge_list, debug_this_test);

	    ASSERT_EQ(edge_list[0], edge_t(1, 2));
	    ASSERT_EQ(edge_list[1], edge_t(1, 2));
	    ASSERT_EQ(edge_list[2], edge_t(1, 3));
	    ASSERT_EQ(edge_list[3], edge_t(1, 6));
	    ASSERT_EQ(edge_list[4], edge_t(2, 4));
	    ASSERT_EQ(edge_list[5], edge_t(2, 5));

   	}
/*
   	TYPED_TEST(TestNewEdgeSwapTFP, manyRandom) {

   		bool debug_this_test = true;
   		using EdgeSwapAlgo = TypeParam;

		const degree_t min_deg = 20;
		const degree_t max_deg = 10000;
		const node_t num_nodes = 1000000;
	    const degree_t threshold = min_deg;
	    
		HavelHakimiIMGenerator hh_gen(HavelHakimiIMGenerator::PushDirection::DecreasingDegree, 0, threshold);
		MonotonicPowerlawRandomStream<false> degreeSequence(min_deg, max_deg, -2, num_nodes);

		StreamPusher<decltype(degreeSequence), decltype(hh_gen)>(degreeSequence, hh_gen);
		hh_gen.generate();

		HavelHakimi_ConfigurationModel<HavelHakimiIMGenerator> cmhh(hh_gen, 
                                                            223224, 
                                                            num_nodes, 
                                                            threshold, 
                                                            hh_gen.maxDegree(), 
                                                            hh_gen.nodesAboveThreshold());

		cmhh.run();

		// for random numbers later
		const node_t edge_count = cmhh.size();

		EdgeVector cmhh_list((unsigned long long int) edge_count);
		EdgeStream edge_stream(true, true);

		stxxl::stream::materialize(cmhh, cmhh_list.begin());

        // get degree distribution of cmhh
        using result_t  = stxxl::vector<decltype(hh_gen)::value_type>;

		DegreeDistributionCheck<result_t::iterator> cmhh_check(cmhh_list.begin(), cmhh_list.end());

        auto cmhh_degreedistribution = cmhh_check.getDistribution();

        //
      	this->_list_to_stream(cmhh_list, edge_stream);

		SwapVector swap_list;

        std::random_device rd;
        std::mt19937_64 gen64(rd());
        std::uniform_int_distribution<node_t> dis(0, edge_count - 1);
        std::bernoulli_distribution disBer(0.5);

        int_t count = 0;

        edge_t prev;
        bool first = true;

        for (stxxl::vector<edge_t>::iterator iter = cmhh_list.begin(); iter != cmhh_list.end(); ++iter) {
            auto edge = *iter;

			if (first) {
				first = false;
				prev = *iter;
				continue;
			}

			if (edge.is_loop() || prev == edge) {
				edgeid_t random_swap_constituent = dis(gen64);
				bool coin = disBer(gen64);

				if (LIKELY(random_swap_constituent != count)) {
                    if (count < random_swap_constituent)
					    swap_list.push_back({count, random_swap_constituent, coin});
                    else
                        swap_list.push_back({random_swap_constituent, count, coin});

					//std::cout << "Added a swap " << count << " , " << random_swap_constituent << " , " << coin << std::endl;
				}
			}
			++count;

		}

		EdgeSwapAlgo algo(edge_stream, swap_list);
	    algo.setDisplayDebug(debug_this_test);

		if (EdgeSwapTrait<EdgeSwapAlgo>::pushableSwaps()) {
	        for (auto &s : swap_list)
	            algo.push(s);
      	}

	    algo.run();

	    EdgeVector edge_list;

	    this->_stream_to_list(edge_stream, edge_list);


        // get degree distribution of edge_list
        using result_t  = stxxl::vector<decltype(edge_stream)::value_type>;

        DegreeDistributionCheck<result_t::iterator> el_check(edge_list.begin(), edge_list.end());

        auto edge_degreedistribution = el_check.getDistribution();

        for (unsigned int i = 0; !edge_degreedistribution.empty() && !cmhh_degreedistribution.empty(); ++i,
                ++edge_degreedistribution, ++cmhh_degreedistribution) {
            ASSERT_EQ((*edge_degreedistribution).value, (*cmhh_degreedistribution).value);
            ASSERT_EQ((*edge_degreedistribution).count, (*cmhh_degreedistribution).count);
        }
        //

	    AlgoFullyInternal esfi(edge_list, swap_list);
		esfi.run();

		auto edge_list_ = esfi.new_edges();

	    for (unsigned int i = 0; i < edge_list.size(); ++i) {
	    	const edge_t e0 = edge_list[i];
	    	const edge_t c0 = edge_list_[i];
	    	ASSERT_EQ(e0, c0);
	    }


        std::cout << "Maximum EM allocation: " <<  stxxl::block_manager::get_instance()->get_maximum_allocation() << std::endl;
    }*/
}
