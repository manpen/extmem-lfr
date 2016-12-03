#include <gtest/gtest.h>

#include <stxxl/vector>
#include <EdgeSwaps/EdgeSwapInternalSwaps.h>
#include <EdgeSwaps/EdgeSwapTFP.h>
#include <EdgeSwaps/EdgeSwapFullyInternal.h>
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
      EdgeSwapInternalSwaps,
      EdgeSwapTFP::EdgeSwapTFP,
      EdgeSwapParallelTFP::EdgeSwapParallelTFP,
//	  EdgeSwapFullyInternal<EdgeVector, SwapVector>,
      IMEdgeSwap
   	>;

    TYPED_TEST_CASE(TestNewEdgeSwapTFP, TestNewEdgeSwapTFPImplementations);

   	TYPED_TEST(TestNewEdgeSwapTFP, conflicts) {
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


   	}
}