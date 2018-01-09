
#include <gtest/gtest.h>

#include <ParallelBufferedPQSorterMerger.h>
#include <stxxl/sorter>
#include <GenericComparator.h>
#include <omp.h>
#include <mutex>

#if 0

class TestParallelBufferedPQSorterMerger : public ::testing::Test {
};

TEST_F(TestParallelBufferedPQSorterMerger, testOrderedData) {
    int_t num_threads = omp_get_max_threads();

    ASSERT_GT(num_threads, 1);

    using sorter_t = stxxl::sorter<edge_t, GenericComparator<edge_t>::Ascending>;

    ParallelBufferedPQSorterMerger<sorter_t, GenericComparator<edge_t>::Descending> pqsortermerger(num_threads);

    #pragma omp parallel num_threads(num_threads)
    {
        int_t tid = omp_get_thread_num();

        pqsortermerger.initialize(tid);

        #pragma omp barrier

        for (int run = 0; run < 4; ++run) {
            pqsortermerger.clear(tid);

            for (int_t i = 0; i < num_threads; ++i) {
                pqsortermerger.push_sorter(edge_t{tid, i}, tid);
            }
            pqsortermerger.finish_sorter_input(tid);

            for (int_t i = num_threads; i < 10*IntScale::M; ++i) {
                pqsortermerger.push_pq(tid, edge_t{i, tid}, i % num_threads);
            }

            #pragma omp barrier

            pqsortermerger.flush_pq_buffer(tid);

            #pragma omp barrier

            auto s = pqsortermerger.get_stream(tid);

            for (int_t i = 0; i < 10*IntScale::M; ++i) {
                if (i%num_threads != tid) {
                    EXPECT_TRUE(s.empty() || s->first > i);
                } else {
                    for (int t = 0; t < num_threads; ++t) {
                        EXPECT_FALSE(s.empty());
                        EXPECT_EQ(s->first, i);
                        EXPECT_EQ(s->second, t);
                        ++s;
                    }
                }
            }

            EXPECT_TRUE(s.empty());
        }
    }
};

TEST_F(TestParallelBufferedPQSorterMerger, testOnlyPQ) {
    int_t num_threads = omp_get_max_threads();

    ASSERT_GT(num_threads, 1);

    using pq_comparator_t = GenericComparator<int64_t>::Descending;
    using PQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<int64_t, pq_comparator_t, 32*IntScale::Mi, 1 << 20>::result;
    using PQBlock = typename PQ::block_type;

    // make sure STXXL is properly initialized before hitting the parallel section
    stxxl::vector<int_t> values;
    for (int64_t i = 0; i < 10*IntScale::M; ++i) {
        values.push_back(i);
    }

    std::mutex m;

    #pragma omp parallel num_threads(2)
    {

        m.lock();
        stxxl::read_write_pool<PQBlock> pq_pool(32*IntScale::Mi / 2 / PQBlock::raw_size, 32*IntScale::Mi / 2 / PQBlock::raw_size);
        PQ pq(pq_pool);
        m.unlock();

        for (int64_t i = 1; i < 10*IntScale::M; ++i) {
            m.lock();
            pq.push(i);
            m.unlock();
        }

        for (int64_t i = 1; i < 10*IntScale::M; ++i) {
            m.lock();
            EXPECT_FALSE(pq.empty());
            EXPECT_EQ(pq.top(), i);
            pq.pop();
            m.unlock();
        }

        EXPECT_TRUE(pq.empty());
    }
};

TEST_F(TestParallelBufferedPQSorterMerger, testOnlyPQSequential) {
    int_t num_threads = omp_get_max_threads();

    ASSERT_GT(num_threads, 1);

    using pq_comparator_t = GenericComparator<int64_t>::Descending;
    using PQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<int64_t, pq_comparator_t, 32*IntScale::Mi, 1 << 20>::result;
    using PQBlock = typename PQ::block_type;

    stxxl::read_write_pool<PQBlock> pq_pool1(32*IntScale::Mi / 2 / PQBlock::raw_size, 32*IntScale::Mi / 2 / PQBlock::raw_size);
    PQ pq1(pq_pool1);
    stxxl::read_write_pool<PQBlock> pq_pool2(32*IntScale::Mi / 2 / PQBlock::raw_size, 32*IntScale::Mi / 2 / PQBlock::raw_size);
    PQ pq2(pq_pool2);

    for (int64_t i = 1; i < 10*IntScale::M; ++i) {
        pq1.push(i);
        pq2.push(i);
    }

    for (int64_t i = 1; i < 10*IntScale::M; ++i) {
        EXPECT_FALSE(pq1.empty());
        EXPECT_FALSE(pq2.empty());
        EXPECT_EQ(pq1.top(), i);
        EXPECT_EQ(pq2.top(), i);
        pq1.pop();
        pq2.pop();
    }

    EXPECT_TRUE(pq1.empty());
    EXPECT_TRUE(pq2.empty());
};


TEST_F(TestParallelBufferedPQSorterMerger, testOnlyPQSequentialConstruction) {
    int_t num_threads = omp_get_max_threads();

    ASSERT_GT(num_threads, 1);

    using pq_comparator_t = GenericComparator<int64_t>::Descending;
    using PQ = typename stxxl::PRIORITY_QUEUE_GENERATOR<int64_t, pq_comparator_t, 32*IntScale::Mi, 1 << 20>::result;
    using PQBlock = typename PQ::block_type;

    stxxl::read_write_pool<PQBlock> pq_pool1(32*IntScale::Mi / 2 / PQBlock::raw_size, 32*IntScale::Mi / 2 / PQBlock::raw_size);
    PQ pq1(pq_pool1);
    stxxl::read_write_pool<PQBlock> pq_pool2(32*IntScale::Mi / 2 / PQBlock::raw_size, 32*IntScale::Mi / 2 / PQBlock::raw_size);
    PQ pq2(pq_pool2);

    std::mutex m;
    omp_set_nested(1);

    #pragma omp parallel num_threads(2)
    {

        PQ* pq;
        if (omp_get_thread_num() == 0) {
            pq = &pq1;
        } else {
            pq = &pq2;
        }

        for (int64_t i = 1; i < 10*IntScale::M; ++i) {
            m.lock();
            #pragma omp master
            pq->push(i);
            m.unlock();
        }

        for (int64_t i = 1; i < 10*IntScale::M; ++i) {
            m.lock();
            EXPECT_FALSE(pq->empty());
            EXPECT_EQ(pq->top(), i);
            pq->pop();
            m.unlock();
        }

        EXPECT_TRUE(pq->empty());
    }
};
#endif