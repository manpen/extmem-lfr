#include <gtest/gtest.h>

#include <stxxl/vector>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <EdgeSwaps/EdgeSwapTFP.h>
#include <EdgeSwaps/EdgeSwapParallelTFP.h>
#include <EdgeSwaps/IMEdgeSwap.h>

namespace {
	using EdgeVector = stxxl::vector<edge_t>;
	using SwapVector = stxxl::vector<SwapDescriptor>;

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

      EdgeSwapInternalSwaps,
      EdgeSwapTFP::EdgeSwapTFP,
      IMEdgeSwap
      //EdgeSwapParallelTFP::EdgeSwapParallelTFP,
   	>;

    TYPED_TEST_CASE(TestNewEdgeSwapTFP, TestNewEdgeSwapTFPImplementations);

   	TYPED_TEST(TestNewEdgeSwapTFP, noConflicts) {
   		bool debug_this_test = true;
   		using EdgeSwapAlgo = TypeParam;

   		EdgeVector edge_list;
   		EdgeStream edge_stream;
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
   		EdgeStream edge_stream;
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
   		swap_list.push_back({0, 5, false});
   		swap_list.push_back({1, 6, false});
   		swap_list.push_back({2, 7, false});
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
   		EdgeStream edge_stream;
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
}