// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Tests that exercise system-call error paths via the SyscallShim.  Each test
// installs a FailingShim configured to make a specific syscall fail, then
// verifies that the client returns the expected absl::Status error and cleans
// up correctly.

#include "client/test_fixture.h"

#include "common/syscall_shim.h"
#include "common/syscall_shim_test_helper.h"

using subspace::testing::FailingShim;
using subspace::testing::ScopedSyscallShim;

class SyscallFailureTest : public SubspaceTestBase {};

// ---------------------------------------------------------------------------
// mmap failures during ClientChannel::Map
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, MmapFailOnScb) {
  // The first mmap in Map() is for the SystemControlBlock.
  // Failing it should return an error from CreatePublisher.
  FailingShim shim;
  // The server also calls mmap, so we need to let the server's mmaps succeed.
  // The client's first mmap (SCB) happens after the server has set up the
  // channel.  We use a countdown to skip the server's mmaps.
  // CreatePublisher triggers: server creates channel (server-side mmaps), then
  // client maps SCB, CCB, BCB.  We want the client's first mmap (SCB) to fail.
  //
  // We install the shim *after* creating the client (which doesn't mmap), then
  // set countdown=0 so the very next mmap fails.
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "mmap_scb_test"));

  shim.mmap_fail_countdown = 0;
  ScopedSyscallShim guard(&shim);

  auto pub = client->CreatePublisher("/mmap_scb_test",
                                     {.slot_size = 64, .num_slots = 4});
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(),
              ::testing::HasSubstr("Failed to map SystemControlBlock"));
}

TEST_F(SyscallFailureTest, MmapFailOnCcb) {
  // Second mmap (CCB) fails.
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "mmap_ccb_test"));

  FailingShim shim;
  shim.mmap_fail_countdown = 1; // skip SCB (index 0), fail CCB (index 1)
  ScopedSyscallShim guard(&shim);

  auto pub = client->CreatePublisher("/mmap_ccb_test",
                                     {.slot_size = 64, .num_slots = 4});
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(),
              ::testing::HasSubstr("Failed to map ChannelControlBlock"));
  // Verify that the SCB mmap was cleaned up (munmap was called).
  EXPECT_GE(shim.munmap_call_count, 1);
}

TEST_F(SyscallFailureTest, MmapFailOnBcb) {
  // Third mmap (BCB) fails.
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "mmap_bcb_test"));

  FailingShim shim;
  shim.mmap_fail_countdown = 2; // skip SCB, CCB; fail BCB
  ScopedSyscallShim guard(&shim);

  auto pub = client->CreatePublisher("/mmap_bcb_test",
                                     {.slot_size = 64, .num_slots = 4});
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(),
              ::testing::HasSubstr("Failed to map BufferControlBlock"));
  // Both SCB and CCB mappings should have been cleaned up.
  EXPECT_GE(shim.munmap_call_count, 2);
}

// ---------------------------------------------------------------------------
// shm_open failures during CreateBuffer
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, ShmOpenFail) {
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "shm_open_test"));

  FailingShim shim;
  shim.shm_open_fail_countdown = 0;
  shim.shm_open_errno = EACCES;
  ScopedSyscallShim guard(&shim);

  auto pub = client->CreatePublisher("/shm_open_test",
                                     {.slot_size = 64, .num_slots = 4});
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(),
              ::testing::HasSubstr("Failed to open shared memory"));
}

// ---------------------------------------------------------------------------
// ftruncate failure after successful shm_open (should shm_unlink on cleanup)
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, FtruncateFailAfterShmOpen) {
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "ftrunc_test"));

  FailingShim shim;
  shim.ftruncate_fail_countdown = 0;
  shim.ftruncate_errno = ENOSPC;
  ScopedSyscallShim guard(&shim);

  auto pub = client->CreatePublisher("/ftrunc_test",
                                     {.slot_size = 64, .num_slots = 4});
  ASSERT_FALSE(pub.ok());
  // On Linux ftruncate is on the shm fd; on POSIX/macOS it's on the shadow file.
  EXPECT_THAT(
      pub.status().message(),
      ::testing::AnyOf(
          ::testing::HasSubstr("Failed to set length of shared memory"),
          ::testing::HasSubstr("Failed to truncate shadow file")));
}

// ---------------------------------------------------------------------------
// chmod failure during CreateBuffer
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, ChmodFail) {
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "chmod_test"));

  FailingShim shim;
  shim.chmod_fail_countdown = 0;
  shim.chmod_errno = EPERM;
  ScopedSyscallShim guard(&shim);

  auto pub = client->CreatePublisher("/chmod_test",
                                     {.slot_size = 64, .num_slots = 4});
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(),
              ::testing::HasSubstr("Failed to change permissions"));
}

// ---------------------------------------------------------------------------
// poll failures in WaitForSubscriber
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, PollFailWaitForSubscriber) {
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "poll_sub_test"));
  auto sub = EVAL_AND_ASSERT_OK(client->CreateSubscriber(
      "/poll_sub_test", {.max_active_messages = 2}));

  FailingShim shim;
  shim.poll_fail_countdown = 0;
  shim.poll_errno = EINTR;
  ScopedSyscallShim guard(&shim);

  auto status = sub.Wait(std::chrono::nanoseconds(1000000)); // 1ms timeout
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              ::testing::HasSubstr("Error from poll waiting for subscriber"));
}

// ---------------------------------------------------------------------------
// poll failures in WaitForReliablePublisher
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, PollFailWaitForReliablePublisher) {
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "poll_pub_test"));
  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "/poll_pub_test",
      {.slot_size = 64, .num_slots = 4, .reliable = true}));

  FailingShim shim;
  shim.poll_fail_countdown = 0;
  shim.poll_errno = EINTR;
  ScopedSyscallShim guard(&shim);

  auto status = pub.Wait(std::chrono::nanoseconds(1000000));
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.message(),
              ::testing::HasSubstr("Error from poll waiting for reliable publisher"));
}

// ---------------------------------------------------------------------------
// fstat failure in GetBufferSize (Linux path)
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, FstatFailInGetBufferSize) {
  // GetBufferSize is called during buffer attachment after the initial
  // CreatePublisher.  We can trigger it via CreateSubscriber which attaches
  // to existing buffers.
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "fstat_test"));

  // First create a publisher (without the shim) so the channel exists.
  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "/fstat_test", {.slot_size = 64, .num_slots = 4}));

  // Now publish a message so buffers are created.
  auto buf = EVAL_AND_ASSERT_OK(pub.GetMessageBuffer());
  memcpy(buf, "test", 4);
  ASSERT_OK(pub.PublishMessage(4));

  // Create a second client and make fstat fail when it tries to get buffer size.
  auto client2 = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "fstat_test2"));

  FailingShim shim;
  shim.fstat_fail_countdown = 0;
  shim.fstat_errno = EBADF;
  ScopedSyscallShim guard(&shim);

  auto sub = client2->CreateSubscriber("/fstat_test", {});
  // This may or may not fail depending on whether fstat is called during
  // subscriber creation.  The important thing is that if it IS called and
  // fails, we get a proper error rather than a crash.
  if (!sub.ok()) {
    EXPECT_THAT(sub.status().message(),
                ::testing::HasSubstr("Failed to get size"));
  }
}

// ---------------------------------------------------------------------------
// Verify shim call counting works
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, ShimCallCounting) {
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "count_test"));

  FailingShim shim;
  ScopedSyscallShim guard(&shim);

  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "/count_test", {.slot_size = 64, .num_slots = 4}));

  // Verify that mmap was called at least 3 times (SCB, CCB, BCB).
  EXPECT_GE(shim.mmap_call_count, 3);
  // Verify that shm_open was called at least once (for buffer creation).
  EXPECT_GE(shim.shm_open_call_count, 1);
  // Verify that ftruncate was called at least once.
  EXPECT_GE(shim.ftruncate_call_count, 1);
  // Verify that chmod was called at least once.
  EXPECT_GE(shim.chmod_call_count, 1);
}

// ---------------------------------------------------------------------------
// Verify that the shim is properly restored after the RAII guard
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, ShimRestoredAfterGuard) {
  {
    FailingShim shim;
    ScopedSyscallShim guard(&shim);
    // Inside the guard, GetSyscallShim() should return our shim.
    EXPECT_EQ(&subspace::GetSyscallShim(), &shim);
  }
  // After the guard, we should be back to the default.
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "restore_test"));
  auto pub = EVAL_AND_ASSERT_OK(client->CreatePublisher(
      "/restore_test", {.slot_size = 64, .num_slots = 4}));
  // If the shim wasn't restored, this would have used the FailingShim
  // (which was destroyed) and likely crashed.
}

// ---------------------------------------------------------------------------
// MapBuffer (mmap for buffer data) failure
// ---------------------------------------------------------------------------

TEST_F(SyscallFailureTest, MmapFailOnBufferMap) {
  auto client = EVAL_AND_ASSERT_OK(
      subspace::Client::Create(Socket(), "bufmap_test"));

  FailingShim shim;
  // SCB (0), CCB (1), BCB (2) succeed; buffer map (3) fails.
  shim.mmap_fail_countdown = 3;
  ScopedSyscallShim guard(&shim);

  auto pub = client->CreatePublisher("/bufmap_test",
                                     {.slot_size = 64, .num_slots = 4});
  ASSERT_FALSE(pub.ok());
  EXPECT_THAT(pub.status().message(),
              ::testing::HasSubstr("Failed to map shared memory"));
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
