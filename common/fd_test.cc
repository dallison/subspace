// All Rights Reserved
// See LICENSE file for licensing information.
#include "common/fd.h"
#include <gtest/gtest.h>
#include <sys/stat.h>

using FileDescriptor = subspace::FileDescriptor;

TEST(FdTest, Create) {
  FileDescriptor fd;
  ASSERT_FALSE(fd.Valid());
  ASSERT_EQ(-1, fd.Fd());
}

TEST(FdTest, CreateWithFd) {
  int f = dup(1);
  FileDescriptor fd(f);
  ASSERT_TRUE(fd.Valid());
  ASSERT_EQ(f, fd.Fd());
}

TEST(FdTest, SetFd) {
  FileDescriptor fd;
  int f = dup(1);
  fd.SetFd(f);
  ASSERT_TRUE(fd.Valid());
  ASSERT_EQ(f, fd.Fd());
}

TEST(FdTest, SetFdSame) {
  FileDescriptor fd;
  int f = dup(1);
  fd.SetFd(f);
  ASSERT_TRUE(fd.Valid());
  ASSERT_EQ(f, fd.Fd());
  fd.SetFd(f);
  ASSERT_TRUE(fd.Valid());
  ASSERT_EQ(f, fd.Fd());
  ASSERT_EQ(1, fd.RefCount());
  // f1 will be open.
  struct stat st;
  int e = fstat(f, &st);
  ASSERT_EQ(0, e);
}

TEST(FdTest, SetFdDiff) {
  FileDescriptor fd;
  int f1 = dup(1);
  int f2 = dup(1);
  fd.SetFd(f1);
  ASSERT_TRUE(fd.Valid());
  ASSERT_EQ(f1, fd.Fd());
  fd.SetFd(f2);
  ASSERT_TRUE(fd.Valid());
  ASSERT_EQ(f2, fd.Fd());
  ASSERT_EQ(1, fd.RefCount());

  // f1 will be closed.
  struct stat st;
  int e = fstat(f1, &st);
  ASSERT_EQ(-1, e);
}

TEST(FdTest, Copy) {
  int f = dup(1);
  FileDescriptor fd1(f);
  FileDescriptor fd2(fd1);
  ASSERT_TRUE(fd1.Valid());
  ASSERT_TRUE(fd2.Valid());
  ASSERT_EQ(f, fd1.Fd());
  ASSERT_EQ(f, fd2.Fd());
  ASSERT_EQ(2, fd1.RefCount());
  ASSERT_EQ(2, fd2.RefCount());
}

TEST(FdTest, CopyAssign) {
  int f = dup(1);
  FileDescriptor fd1(f);
  FileDescriptor fd2;
  fd2 = fd1;
  ASSERT_TRUE(fd1.Valid());
  ASSERT_TRUE(fd2.Valid());
  ASSERT_EQ(f, fd1.Fd());
  ASSERT_EQ(f, fd2.Fd());
  ASSERT_EQ(2, fd1.RefCount());
  ASSERT_EQ(2, fd2.RefCount());
}

TEST(FdTest, Move) {
  int f = dup(1);
  FileDescriptor fd1(f);
  FileDescriptor fd2(std::move(fd1));
  ASSERT_FALSE(fd1.Valid());
  ASSERT_TRUE(fd2.Valid());
  ASSERT_EQ(-1, fd1.Fd());
  ASSERT_EQ(f, fd2.Fd());
  ASSERT_EQ(0, fd1.RefCount());
  ASSERT_EQ(1, fd2.RefCount());
}

TEST(FdTest, MoveAssign) {
  int f = dup(1);
  FileDescriptor fd1(f);
  FileDescriptor fd2;
  fd2 = (std::move(fd1));
  ASSERT_FALSE(fd1.Valid());
  ASSERT_TRUE(fd2.Valid());
  ASSERT_EQ(-1, fd1.Fd());
  ASSERT_EQ(f, fd2.Fd());
  ASSERT_EQ(0, fd1.RefCount());
  ASSERT_EQ(1, fd2.RefCount());
}

void WithFd(FileDescriptor f, int fd, int refs) {
  ASSERT_EQ(fd, f.Fd());
  ASSERT_EQ(refs, f.RefCount());
}

TEST(FdTest, PassAsArg) {
  int f = dup(1);
  FileDescriptor fd(f);
  WithFd(fd, f, 2);

  // f will be open.
  struct stat st;
  int e = fstat(f, &st);
  ASSERT_EQ(0, e);
}

TEST(FdTest, PassMove) {
  int f = dup(1);
  FileDescriptor fd(f);
  WithFd(std::move(fd), f, 1);

  // f will be closed.
  struct stat st;
  int e = fstat(f, &st);
  ASSERT_EQ(-1, e);
}
