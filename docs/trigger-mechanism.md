# Choosing how a publisher wakes a subscriber

A publisher wakes a subscriber that is waiting for a message. The default
mechanism is an eventfd on Linux, a Unix-domain socketpair on QNX, and a pipe
everywhere else. QNX pipes go through a single-threaded pipe service. A
socketpair is slower than a pipe on macOS, and its read end always reports
`POLLOUT`.

`default` keeps that platform choice. `eventfd`, `pipe`, and `socketpair`
select one mechanism for the whole build. `eventfd` compiles only on Linux.
The choice is process-local. It does not change shared memory, so clients and
the server do not have to agree.

## Bazel

In this repository:

```bash
bazel build --//:trigger_mechanism=socketpair //server
```

`eventfd`, `pipe`, and `socketpair` are the other values. Leaving the flag
unset, or setting it to `default`, keeps the platform choice.

A downstream module uses the same flag through the Subspace repo name:

```bash
bazel build --@subspace//:trigger_mechanism=socketpair //your:target
```

Check the compile of the trigger implementation:

```bash
bazel aquery 'mnemonic(CppCompile, @cpp_toolbelt//toolbelt:toolbelt)' | grep TOOLBELT_TRIGGERFD_SIGNAL_MECHANISM
```

`default` produces no `TOOLBELT_TRIGGERFD_SIGNAL_MECHANISM` define. `pipe`
produces `-DTOOLBELT_TRIGGERFD_SIGNAL_MECHANISM=TOOLBELT_TRIGGERFD_PIPE`.

## CMake

```bash
cmake -B build -DSUBSPACE_TRIGGER_MECHANISM=socketpair
cmake --build build
```

`SUBSPACE_TRIGGER_MECHANISM` is `default`, `eventfd`, `pipe`, or `socketpair`.

## Regression tests

`//socketpair` rebuilds these tests with `--//:trigger_mechanism=socketpair`:

- `//client:client_test` (`//socketpair:client_test_socketpair`), on Linux and macOS
- `//c_client:client_test`, Linux only
- `//client:bridge_test`, Linux only
- `//server:server_test`, Linux only
- the three `//co20_rpc` tests, Linux only

`bazel test //...` runs `client_test_socketpair` on the Linux and macOS CI
jobs. Other platforms skip it, and the other wrappers are Linux only. A
socketpair reports `POLLOUT` while it is open, so a test hangs if a `POLLIN`
waiter is resumed for that event.
