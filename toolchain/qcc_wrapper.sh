#!/bin/bash
# subspace/toolchain/qcc_wrapper.sh
export QNX_HOST=$HOME/qnx800/host/linux/x86_64
export QNX_TARGET=$HOME/qnx800/target/qnx8
export PATH=$QNX_HOST/usr/bin:$PATH

# -V specifies the compiler/arch for QNX 8
exec qcc -Vgcc_ntoaarch64le "$@"
