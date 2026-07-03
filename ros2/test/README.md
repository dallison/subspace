# Subspace ROS 2 Overlay Tests

Run these tests from a sourced Jazzy overlay workspace built with the packages in
`ros2/`.

## Smoke Test

```bash
/path/to/subspace/ros2/test/run_overlay_smoke.sh /path/to/jazzy_overlay_workspace
```

This test starts `subspace_server`, enables CycloneDDS shared memory with
`ros2/cyclonedds_subspace.xml`, runs `demo_nodes_cpp` talker/listener, and runs
the `add_two_ints` service client/server.

## Manual Validation Matrix

- Same-host pub/sub: run the smoke test and confirm the listener receives
  samples while `CYCLONEDDS_URI` enables shared memory.
- Same-host service traffic: run the smoke test and confirm the service client
  receives `Result of add_two_ints`.
- Mixed local/remote delivery: run a talker on host A, a listener on host A, and
  a listener on host B. The host A listener should receive through Subspace, and
  the host B listener should continue to receive through DDS networking.
- Subspace outage fallback: run talker/listener with shared memory enabled but
  without `subspace_server`. Endpoint creation should succeed and samples should
  still flow over normal DDS delivery.
- Duplicate delivery: run one local listener with shared memory enabled and
  verify each sequence number is observed once.
- Loaned-message path: run `demo_nodes_cpp talker_loaned_message` with a fixed
  size message and confirm samples are delivered and returned without exhausting
  Subspace slots.
- Performance comparison: compare stock `rmw_cyclonedds_cpp`, CycloneDDS with
  real Iceoryx where available, and this Subspace-backed compatibility layer on
  fixed-size and serialized dynamic messages.
