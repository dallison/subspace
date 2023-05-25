// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include "common/hexdump.h"

#include <cctype>
#include <cstdio>

namespace subspace {

void Hexdump(const void *addr, size_t length) {
  const char *p = reinterpret_cast<const char *>(addr);
  length = (length + 15) & ~15;
  while (length > 0) {
    printf("%p ", p);
    for (int i = 0; i < 16; i++) {
      printf("%02X ", p[i] & 0xff);
    }
    printf("  ");
    for (int i = 0; i < 16; i++) {
      if (isprint(p[i])) {
        printf("%c", p[i]);
      } else {
        printf(".");
      }
    }
    printf("\n");
    p += 16;
    length -= 16;
  }
}

}  // namespace subspace