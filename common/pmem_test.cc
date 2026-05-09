// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/pmem.h"

#include "gtest/gtest.h"

#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace subspace {
namespace {

PmemBufferMetadata TestMetadata(const std::string &suffix) {
  PmemBufferMetadata metadata;
  metadata.channel_name = "/pmem_test";
  metadata.session_id = static_cast<uint64_t>(getpid());
  metadata.buffer_index = 1;
  metadata.full_size = 123;
  metadata.allocation_size = 4096;
  metadata.pmem_handle = 1234;
  metadata.shadow_file =
      "/tmp/subspace_pmem_test_" + std::to_string(getpid()) + "_" + suffix;
  metadata.object_name = QnxPmemObjectName(metadata.shadow_file);
#if (defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)) ||            \
    (defined(__linux__) && defined(SUBSPACE_ENABLE_LINUX_PMEM_SHIM))
  metadata.slot_id = 2;
  metadata.is_prefix = false;
  metadata.pmem_alignment = 4096;
  metadata.pmem_pool_id = "test-pool";
  metadata.pmem_cache_enabled = true;
#endif
  return metadata;
}

TEST(PmemTest, WritesAndReadsMetadata) {
  PmemBufferMetadata metadata = TestMetadata("metadata");
  (void)DestroyQnxPmemBuffer(metadata);

  ASSERT_TRUE(WritePmemMetadataFile(metadata).ok());
  auto read_metadata = ReadPmemMetadataFile(metadata.shadow_file);
  ASSERT_TRUE(read_metadata.ok()) << read_metadata.status();

  EXPECT_EQ(read_metadata->channel_name, metadata.channel_name);
  EXPECT_EQ(read_metadata->session_id, metadata.session_id);
  EXPECT_EQ(read_metadata->buffer_index, metadata.buffer_index);
  EXPECT_EQ(read_metadata->full_size, metadata.full_size);
  EXPECT_EQ(read_metadata->allocation_size, metadata.allocation_size);
  EXPECT_EQ(read_metadata->pmem_handle, metadata.pmem_handle);
  EXPECT_EQ(read_metadata->shadow_file, metadata.shadow_file);
  EXPECT_EQ(read_metadata->object_name, metadata.object_name);
#if (defined(__QNXNTO__) && defined(SUBSPACE_ENABLE_QNX_PMEM)) ||            \
    (defined(__linux__) && defined(SUBSPACE_ENABLE_LINUX_PMEM_SHIM))
  EXPECT_EQ(read_metadata->slot_id, metadata.slot_id);
  EXPECT_EQ(read_metadata->is_prefix, metadata.is_prefix);
  EXPECT_EQ(read_metadata->pmem_alignment, metadata.pmem_alignment);
  EXPECT_EQ(read_metadata->pmem_pool_id, metadata.pmem_pool_id);
  EXPECT_EQ(read_metadata->pmem_cache_enabled, metadata.pmem_cache_enabled);
#endif

  (void)DestroyQnxPmemBuffer(metadata);
}

TEST(PmemTest, CreatesOpensAndDestroysObject) {
  PmemBufferMetadata metadata = TestMetadata("object");
  (void)DestroyQnxPmemBuffer(metadata);

  auto created = CreateQnxPmemBuffer(metadata);
  ASSERT_TRUE(created.ok()) << created.status();
  ASSERT_TRUE(created->Valid());

  ASSERT_TRUE(WritePmemMetadataFile(metadata).ok());
  auto opened = OpenQnxPmemBuffer(metadata, O_RDWR);
  ASSERT_TRUE(opened.ok()) << opened.status();
  ASSERT_TRUE(opened->Valid());

  struct stat sb;
  ASSERT_EQ(fstat(opened->Fd(), &sb), 0);
  EXPECT_EQ(static_cast<uint64_t>(sb.st_size), metadata.allocation_size);

  ASSERT_TRUE(DestroyQnxPmemBuffer(metadata).ok());
  EXPECT_FALSE(OpenQnxPmemBuffer(metadata, O_RDWR).ok());
}

} // namespace
} // namespace subspace
