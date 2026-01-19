#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "../src/block/block.h"
#include "../src/block/block_cache.h"

using namespace ::LSMT;

class BlockCacheTest : public ::testing::Test {
protected:
    void SetUp() override {
        block_cache = std::make_unique<BlockCache>(3,2);
    }
    std::unique_ptr<BlockCache> block_cache;
};

TEST_F(BlockCacheTest, PutAndGet) {
    auto block1 = std::make_shared<Block>();
    auto block2 = std::make_shared<Block>();
    auto block3 = std::make_shared<Block>();

    block_cache->put(1, 1, block1);
    block_cache->put(1, 2, block2);
    block_cache->put(1, 3, block3);

    EXPECT_EQ(block_cache->get(1, 1), block1);
    EXPECT_EQ(block_cache->get(1, 2), block2);
    EXPECT_EQ(block_cache->get(1, 3), block3);
}

TEST_F(BlockCacheTest, CacheEviction1) {
    auto block1 = std::make_shared<Block>();
    auto block2 = std::make_shared<Block>();
    auto block3 = std::make_shared<Block>();
    auto block4 = std::make_shared<Block>();

    block_cache->put(1, 1, block1);
    block_cache->put(1, 2, block2);
    block_cache->put(1, 3, block3);

    block_cache->get(1, 1);
    block_cache->get(1, 2);

    block_cache->put(1, 4, block4);

    EXPECT_EQ(block_cache->get(1, 1), block1);
    EXPECT_EQ(block_cache->get(1, 2), block2);
    EXPECT_EQ(block_cache->get(1, 3), nullptr);
    EXPECT_EQ(block_cache->get(1, 4), block4);
}

TEST_F(BlockCacheTest, CacheEviction2) {
    auto block1 = std::make_shared<Block>();
    auto block2 = std::make_shared<Block>();
    auto block3 = std::make_shared<Block>();
    auto block4 = std::make_shared<Block>();

    block_cache->put(1, 1, block1);
    block_cache->put(1, 2, block2);
    block_cache->put(1, 3, block3);

    block_cache->get(1, 1);
    block_cache->get(1, 2);
    block_cache->get(1, 3);

    block_cache->put(1, 4, block4);

    EXPECT_EQ(block_cache->get(1, 1), nullptr);
    EXPECT_EQ(block_cache->get(1, 2), block2);
    EXPECT_EQ(block_cache->get(1, 3), block3);
    EXPECT_EQ(block_cache->get(1, 4), block4);
}

TEST_F(BlockCacheTest, HitRate) {
    auto block1 = std::make_shared<Block>();
    auto block2 = std::make_shared<Block>();

    block_cache->put(1, 1, block1);
    block_cache->put(1, 2, block2);

    block_cache->get(1, 1);
    block_cache->get(1, 2);

    block_cache->get(1, 3);

    EXPECT_EQ(block_cache->hit_rate(), 2.0 / 3.0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}