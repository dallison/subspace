// Copyright 2023-2026 David Allison
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

.global SYM(SubspaceCRC32)
SYM_DATA(SubspaceCRC32)

// Entry:
// w0: input crc
// x1: address of start of buffer
// x2: length of buffer
//
// Exit:
// x0: new crc
SYM(SubspaceCRC32):
1:
  // 8-byte loop until x2 is less than 8
  cmp x2, #8
  blt 2f
  ldr x4, [x1], #8
  crc32x w0, w0, x4
  subs x2, x2, #8
  b 1b

  // 4 byte remainder.  Can only be one.
2:
  cmp x2, #4
  blt 3f
  ldr w4, [x1], #4
  crc32w w0, w0, w4
  subs x2, x2, #4

  // Single byte remainder.
3:
  cbz x2, 4f
  ldrb w4, [x1], #1
  crc32b w0, w0, w4
  subs x2, x2, #1
  b 3b
4:
  ret

#endif
