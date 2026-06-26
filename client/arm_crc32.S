// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#if defined(__ARM_FEATURE_CRC32) && defined(__aarch64__)

// This is a handcoded ARM64 CRC32 function that uses the built-in CRC
// instructions.  It is as fast as I can make it.

#if defined(__APPLE__)
#define SYM(name) _##name
#define SYM_DATA(name) .p2align 2
.section        __TEXT,__text,regular,pure_instructions

#elif defined(__linux__) || defined(__QNX__) || defined(__QNXNTO__)
.text
#define SYM(name) #name
#define SYM_DATA(name) .type SYM(name), @function
#else
#error "Unknown OS"
#endif

.global SYM(SubspaceCRC32ArmParallel)
.global SYM(SubspaceCRC32)
.p2align 4
SYM_DATA(SubspaceCRC32)

// Entry:
// w0: input crc
// x1: address of start of buffer
// x2: length of buffer
//
// Exit:
// x0: new crc
SYM(SubspaceCRC32):
  // Keep this threshold in sync with kMinParallelCRC32Bytes in checksum.cc.
  cmp x2, #0x40000
  b.hs 0f

.global SYM(SubspaceCRC32ArmScalar)
SYM_DATA(SubspaceCRC32ArmScalar)

// Entry:
// w0: input crc
// x1: address of start of buffer
// x2: length of buffer
//
// Exit:
// x0: new crc
SYM(SubspaceCRC32ArmScalar):
1:
  // 16-byte loop until x2 is less than 16.
  cmp x2, #16
  blt 2f
  ldp x4, x5, [x1], #16
  crc32x w0, w0, x4
  crc32x w0, w0, x5
  subs x2, x2, #16
  b 1b

2:
  cbz x2, 6f

  // 8-byte remainder.
  tbz x2, #3, 3f
  ldr x4, [x1], #8
  crc32x w0, w0, x4

  // 4-byte remainder.
3:
  tbz x2, #2, 4f
  ldr w4, [x1], #4
  crc32w w0, w0, w4

  // 2-byte remainder.
4:
  tbz x2, #1, 5f
  ldrh w4, [x1], #2
  crc32h w0, w0, w4

  // 1-byte remainder.
5:
  tbz x2, #0, 6f
  ldrb w4, [x1]
  crc32b w0, w0, w4
6:
  ret

0:
  b SYM(SubspaceCRC32ArmParallel)

.global SYM(SubspaceCRC32ArmFourLane)
.p2align 4
SYM_DATA(SubspaceCRC32ArmFourLane)

// Entry:
// x0: address of start of lane 0
// x1: address of start of lane 1
// x2: address of start of lane 2
// x3: address of start of lane 3
// x4: length of each lane, in bytes
// x5: pointer to crc for lane 0
// x6: pointer to crc for lane 1
// x7: pointer to crc for lane 2
// [sp]: pointer to crc for lane 3
SYM(SubspaceCRC32ArmFourLane):
  ldr x8, [sp]
  ldr w9, [x5]
  ldr w10, [x6]
  ldr w11, [x7]
  ldr w12, [x8]

1:
  cmp x4, #16
  blt 2f

  ldp x13, x14, [x0], #16
  ldp x15, x16, [x1], #16
  crc32x w9, w9, x13
  crc32x w10, w10, x15
  crc32x w9, w9, x14
  crc32x w10, w10, x16

  ldp x13, x14, [x2], #16
  ldp x15, x16, [x3], #16
  crc32x w11, w11, x13
  crc32x w12, w12, x15
  crc32x w11, w11, x14
  crc32x w12, w12, x16

  subs x4, x4, #16
  b 1b

2:
  tbz x4, #3, 3f
  ldr x13, [x0], #8
  ldr x14, [x1], #8
  ldr x15, [x2], #8
  ldr x16, [x3], #8
  crc32x w9, w9, x13
  crc32x w10, w10, x14
  crc32x w11, w11, x15
  crc32x w12, w12, x16

3:
  str w9, [x5]
  str w10, [x6]
  str w11, [x7]
  str w12, [x8]
  ret

#endif
