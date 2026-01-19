#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "../src/block/block_meta.h"

using namespace ::LSMT;

class BlockMetaTest : public ::testing::Test {
protected:
    std::vector<BlockMeta> CreateTestMeta() {
        std::vector<BlockMeta> entries;
        entries.emplace_back(0, "A000", "A999");
        entries.emplace_back(100, "B000", "B999");
        entries.emplace_back(200, "C000", "C999");
        return entries;
    }
};

TEST_F(BlockMetaTest, EncodeAndDecodeTest) {
    std::vector<BlockMeta> entries = CreateTestMeta();
    std::vector<BlockMeta> decoded;
    std::vector<uint8_t> encoded;

    BlockMeta::encode_meta(entries, encoded);
    BlockMeta::decode_meta(encoded, decoded);

    ASSERT_EQ(entries.size(), decoded.size());
    for (size_t i = 0; i < entries.size(); ++i) {
        EXPECT_EQ(entries[i].offset, decoded[i].offset);
        EXPECT_EQ(entries[i].fkey, decoded[i].fkey);
        EXPECT_EQ(entries[i].lkey, decoded[i].lkey);
    }
}

TEST_F(BlockMetaTest, EmptyMetaTest) {
    std::vector<BlockMeta> entries;
    std::vector<BlockMeta> decoded;
    std::vector<uint8_t> encoded;

    BlockMeta::encode_meta(entries, encoded);
    EXPECT_FALSE(encoded.empty());
    BlockMeta::decode_meta(encoded, decoded);
    EXPECT_TRUE(decoded.empty());
}

TEST_F(BlockMetaTest, ErrorMetaTest) {
    std::vector<BlockMeta> entries;

    std::vector<uint8_t> error_data = {1, 2, 3};
    EXPECT_THROW(BlockMeta::decode_meta(error_data, entries), std::runtime_error);

    std::vector<uint8_t> empty_data;
    EXPECT_THROW(BlockMeta::decode_meta(empty_data, entries), std::runtime_error);

    std::vector<uint8_t> hash_error;
    entries = CreateTestMeta();
    BlockMeta::encode_meta(entries, hash_error);
    hash_error.back() ^= 1;
    EXPECT_THROW(BlockMeta::decode_meta(hash_error, entries), std::runtime_error);
}

TEST_F(BlockMetaTest, SpecialMetaTest) {
    std::vector<BlockMeta> entries;
    std::vector<BlockMeta> decoded;
    std::vector<uint8_t> encoded;

    entries.emplace_back(0,  std::string("fkey\0with\0null", 14),  std::string("lkey\0with\0null", 14));
    BlockMeta::encode_meta(entries, encoded);
    BlockMeta::decode_meta(encoded, decoded);
    ASSERT_EQ(decoded.size(), entries.size());
    EXPECT_EQ(decoded[0].fkey, std::string("fkey\0with\0null", 14));
    EXPECT_EQ(decoded[0].lkey, std::string("lkey\0with\0null", 14));
}

TEST_F(BlockMetaTest, LargeMetaTest) {
    std::vector<BlockMeta> entries;
    std::vector<BlockMeta> deocded;
    std::vector<uint8_t> encoded;
    
    for (int i = 0; i < 1000; ++i) {
        size_t offset = i * 100;
        char fkey[16], lkey[16];
        snprintf(fkey, sizeof(fkey), "key%03d00", i);  // key00000 key00100 key00200 ...
        snprintf(lkey, sizeof(lkey), "key%03d99", i);  // key00099 key00199 key00299 ...
        entries.emplace_back(offset, fkey, lkey);
    }
    BlockMeta::encode_meta(entries, encoded);
    BlockMeta::decode_meta(encoded, deocded);

    ASSERT_EQ(deocded.size(), entries.size());
    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(deocded[i].offset, entries[i].offset);
        EXPECT_EQ(deocded[i].fkey, entries[i].fkey);
        EXPECT_EQ(deocded[i].lkey, entries[i].lkey);
    }
    for (int i = 0; i < 1000; ++i) {
        EXPECT_LT(deocded[i - 1].lkey, deocded[i].fkey);
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}