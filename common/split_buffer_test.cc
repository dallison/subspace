// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/split_buffer.h"

#include "gtest/gtest.h"

#include <fcntl.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace subspace {
namespace {

std::string UniqueShadowFile(const std::string &suffix) {
  std::string path = "/tmp/subspace_split_buffer_test_" + suffix + "_XXXXXX";
  std::vector<char> buffer(path.begin(), path.end());
  buffer.push_back('\0');
  int fd = mkstemp(buffer.data());
  if (fd >= 0) {
    close(fd);
    unlink(buffer.data());
  }
  return std::string(buffer.data());
}

SplitBufferMetadata TestMetadata(const std::string &suffix) {
  SplitBufferMetadata metadata;
  metadata.channel_name = "/split_buffer_test";
  metadata.session_id = static_cast<uint64_t>(getpid());
  metadata.buffer_index = 1;
  metadata.slot_id = 2;
  metadata.is_prefix = false;
  metadata.full_size = 123;
  metadata.allocation_size = 4096;
  metadata.handle = 1234;
  metadata.shadow_file = UniqueShadowFile(suffix);
  metadata.object_name = SplitBufferObjectName(metadata.shadow_file);
  return metadata;
}

TEST(SplitBufferTest, WritesAndReadsMetadata) {
  SplitBufferMetadata metadata = TestMetadata("metadata");
  (void)DestroySplitSharedMemoryBuffer(metadata);

  ASSERT_TRUE(WriteSplitBufferMetadataFile(metadata).ok());
  auto read_metadata = ReadSplitBufferMetadataFile(metadata.shadow_file);
  ASSERT_TRUE(read_metadata.ok()) << read_metadata.status();

  EXPECT_EQ(read_metadata->channel_name, metadata.channel_name);
  EXPECT_EQ(read_metadata->session_id, metadata.session_id);
  EXPECT_EQ(read_metadata->buffer_index, metadata.buffer_index);
  EXPECT_EQ(read_metadata->slot_id, metadata.slot_id);
  EXPECT_EQ(read_metadata->is_prefix, metadata.is_prefix);
  EXPECT_EQ(read_metadata->full_size, metadata.full_size);
  EXPECT_EQ(read_metadata->allocation_size, metadata.allocation_size);
  EXPECT_EQ(read_metadata->handle, metadata.handle);
  EXPECT_EQ(read_metadata->shadow_file, metadata.shadow_file);
  EXPECT_EQ(read_metadata->object_name, metadata.object_name);

  (void)DestroySplitSharedMemoryBuffer(metadata);
}

TEST(SplitBufferTest, CreatesOpensAndDestroysSharedMemoryObject) {
  SplitBufferMetadata metadata = TestMetadata("object");
  (void)DestroySplitSharedMemoryBuffer(metadata);

  auto created = CreateSplitSharedMemoryBuffer(metadata);
  ASSERT_TRUE(created.ok()) << created.status();
  ASSERT_TRUE(created->Valid());

  ASSERT_TRUE(WriteSplitBufferMetadataFile(metadata).ok());
  auto opened = OpenSplitSharedMemoryBuffer(metadata, O_RDWR);
  ASSERT_TRUE(opened.ok()) << opened.status();
  ASSERT_TRUE(opened->Valid());

  struct stat sb;
  ASSERT_EQ(fstat(opened->Fd(), &sb), 0);
  EXPECT_GE(static_cast<uint64_t>(sb.st_size), metadata.allocation_size);

  ASSERT_TRUE(DestroySplitSharedMemoryBuffer(metadata).ok());
  EXPECT_FALSE(OpenSplitSharedMemoryBuffer(metadata, O_RDWR).ok());
}

} // namespace
} // namespace subspace
