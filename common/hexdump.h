// Copyright 2023 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef __COMMON_HEXDUMP_H
#define __COMMON_HEXDUMP_H

#include <stddef.h>

namespace subspace {
// Almost the first thing I write in a new project is a hexdump
// function.  Very useful.
void Hexdump(const void* addr, size_t length);
}  // namespace subspace

#endif  // __COMMON_HEXDUMP_H