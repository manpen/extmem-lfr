
#include <gtest/gtest.h>

#include <ParallelBufferedPQSorterMerger.h>
#include <stxxl/sorter>
#include <GenericComparator.h>
#include <omp.h>

class TestParallelBufferedPQSorterMerger : public ::testing::Test {
};

TEST_F(TestParallelBufferedPQSorterMerger, testOrderedData) {
    int_t num_threads = omp_get_max_threads();

    ASSERT_GT(num_threads, 1);

    using sorter_t = stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending>;
    std::vector<std::unique_ptr<sorter_t>> sorters(num_threads);
    for (int_t i = 0; i < num_threads; ++i) {
        sorters[i].reset(new sorter_t(GenericComparator<edge_t>::Ascending(), SORTER_MEM));
        sorters[i]->sort();
    }

    ParallelBufferedPQSorterMerger<sorter_t, GenericComparator<edge_t>::Descending> pqsortermerger(num_threads, sorters);

    #pragma omp parallel num_threads(num_threads)
    {
        int_t tid = omp_get_thread_num();
        ParallelBufferedPQSorterMerger<sorter_t, GenericComparator<edge_t>::Descending>::ThreadData local_pqsortermerger(pqsortermerger, tid);

        for (int run = 0; run < 4; ++run) {
            for (int_t i = 0; i < 1000; ++i) {
                local_pqsortermerger.push(edge_t{i, tid}, i % num_threads);
            }

            #pragma omp barrier

            local_pqsortermerger.flush_buffer();

            #pragma omp barrier


            for (int_t i = 0; i < 1000; ++i) {
                if (i%num_threads != tid) {
                    EXPECT_TRUE(local_pqsortermerger.empty() || local_pqsortermerger->first > i);
                } else {
                    for (int t = 0; t < num_threads; ++t) {
                        EXPECT_FALSE(local_pqsortermerger.empty());
                        EXPECT_EQ(local_pqsortermerger->first, i);
                        EXPECT_EQ(local_pqsortermerger->second, t);
                        ++local_pqsortermerger;
                    }
                }
            }

            EXPECT_TRUE(local_pqsortermerger.empty());
        }
    }
};