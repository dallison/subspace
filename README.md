# Subspace IPC
Next Generation, sub-microsecond latency shared memory IPC.

This is a shared-memory based pub/sub Interprocess Communication system that can be used
in robotics and other applications.  Why *subspace*?  If your messages are transported
between processes on the same computer, they travel through extremely low latency
and high bandwidth shared memory shared memory buffers, kind of like they are going
faster than light (not really, of course).  If they go between computers, they are
transported over the network at sub-light speed.

It has the following features:

1.	Single threaded coroutine based server process written in C++17
1.	Coroutine-aware client library, in C++17.
1.	Publish/subscribe methodology with multiple publisher and multiple subscribers per channel.
1.	No communication with server for message transfer.
1.	Message type agnostic transmission – bring your own serialization.
1.  Channel types, meaningful to user, not system.
1.	Single lock POSIX shared memory channels
1.	Both unreliable and reliable communications between publishers and subscribers.
1.	Ability to read the next or newest message in a channel.
1.	File-descriptor-based event triggers.
1.	Automatic UDP discovery and TCP bridging of channels between servers.
1.	Shared and weak pointers for message references.
1.	Ports to MacOS and Linux, ARM64 and x86_64.
1.	Builds using Bazel and uses Abseil and Protocol Buffers from Google.
1.	Uses my C++ coroutine library (https://github.com/dallison/cocpp)

See the file docs/subspace.pdf for full documentation.

# Building
This uses Google's Bazel to build.  You will need to download Bazel to build it.
The build also needs some external libraries, but Bazel takes care of downloading them.
The *.bazelrc* file contains some configuration options.

## To build on Mac Apple Silicon
```
bazel build --config=apple_silicon ...
```

## To build on Linux
Subspace really wants to be built using *clang*.  Depending on how your OS is configured, you
might need to tell bazel what compiler to use.

```
CC=clang bazel build ...
```

It does build with *g++* but you will get some compiler warnings about different signed comparisons
that clang doesn't care about.

