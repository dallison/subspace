# Copyright 2026 General Motors Inc.
# All Rights Reserved
# See LICENSE file for licensing information.

# Java wrapper for Android clients. The classes use JNI but do not depend on
# Android SDK APIs, so they can be compiled with the host Java toolchain.

find_package(Java COMPONENTS Development QUIET)

if(Java_FOUND)
    include(UseJava)

    file(GLOB SUBSPACE_JAVA_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/java/com/subspace/*.java"
    )

    add_jar(subspace_java
        SOURCES ${SUBSPACE_JAVA_SOURCES}
        OUTPUT_NAME subspace-java
    )

    file(GLOB SUBSPACE_JAVA_TEST_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/java/com/subspace/test/*.java"
    )

    add_jar(subspace_java_test
        SOURCES ${SUBSPACE_JAVA_TEST_SOURCES}
        INCLUDE_JARS subspace_java
        ENTRY_POINT com.subspace.test.SubspaceJavaClientTest
        OUTPUT_NAME subspace-java-test
    )
else()
    message(STATUS "Java development tools not found; skipping subspace_java")
endif()

if(ANDROID)
    add_library(subspace_jni SHARED
        jni/subspace_jni.cc
    )

    target_link_libraries(subspace_jni PRIVATE
        subspace_client
        subspace_common
        log
    )
endif()
