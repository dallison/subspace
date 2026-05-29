#!/system/bin/sh

# Copyright 2026 General Motors Inc.
# All Rights Reserved
# See LICENSE file for licensing information.

base=/system
export CLASSPATH=$base/framework/subspace_java_client_test.jar
export SUBSPACE_TEST_WORK_DIR=${SUBSPACE_TEST_WORK_DIR:-/data/local/tmp}
export SUBSPACE_SERVER=${SUBSPACE_SERVER:-$base/bin/subspace_server}
export SUBSPACE_SOCKET=${SUBSPACE_SOCKET:-/data/local/tmp/subspace}

exec app_process $base/bin com.subspace.test.SubspaceJavaClientTest "$@"
