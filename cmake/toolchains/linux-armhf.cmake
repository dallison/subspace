# Cross-compile Subspace for 32-bit ARM Linux (armhf / armv7).
#
# Intended to run on a Linux aarch64 or x86_64 host, including an Ubuntu
# container on the Colima aarch64 VM. The compilers come from the
# gcc-arm-linux-gnueabihf packages. 64-bit atomics are out of line on armhf,
# so every final link pulls in libatomic.
#
# Tests are launched with qemu-arm-static. -L points it at the armhf sysroot
# so the dynamic loader and libstdc++ resolve.

set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR arm)

set(CMAKE_C_COMPILER arm-linux-gnueabihf-gcc)
set(CMAKE_CXX_COMPILER arm-linux-gnueabihf-g++)

set(CMAKE_FIND_ROOT_PATH /usr/arm-linux-gnueabihf)
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)

set(CMAKE_CROSSCOMPILING_EMULATOR qemu-arm-static -L /usr/arm-linux-gnueabihf)

set(CMAKE_EXE_LINKER_FLAGS_INIT "-latomic")
set(CMAKE_SHARED_LINKER_FLAGS_INIT "-latomic")
set(CMAKE_MODULE_LINKER_FLAGS_INIT "-latomic")
