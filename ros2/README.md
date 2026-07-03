# ROS 2 Jazzy Subspace Local IPC Overlay

This directory contains the ROS 2 overlay pieces for using Subspace directly
inside CycloneDDS for same-host shared-memory delivery.

The approach keeps `rmw_cyclonedds_cpp` as the ROS RMW. CycloneDDS still owns
DDS discovery, QoS matching, graph behavior, services, actions, and remote
traffic. The CycloneDDS patch in `ros2/patches/cyclonedds-direct-subspace.patch`
replaces the Iceoryx-backed SHM implementation with a direct Subspace backend
that links `subspace_client`.

## Workspace Layout

From a Jazzy workspace:

```bash
vcs import src < /path/to/subspace/ros2/ros2_subspace_jazzy.repos
ln -s /path/to/subspace/ros2/subspace_vendor src/subspace_vendor
git -C src/eclipse-cyclonedds/cyclonedds apply /path/to/subspace/ros2/patches/cyclonedds-direct-subspace.patch
git -C src/ros2/rmw_cyclonedds apply /path/to/subspace/ros2/patches/rmw-cyclonedds-direct-subspace.patch
SUBSPACE_SOURCE_DIR=/path/to/subspace colcon build --packages-up-to rmw_cyclonedds_cpp
```

Run `subspace_server` before starting ROS nodes that enable CycloneDDS shared
memory. CycloneDDS discovers local readers and writers through DDS as usual; the
shared-memory publication and subscription calls are served by Subspace.

## Subspace Shared-Memory Sizing

Subspace sizing is configured in CycloneDDS XML under
`Domain/SharedMemory/Subspace`:

```xml
<Domain Id="any">
  <SharedMemory>
    <Enable>true</Enable>
    <Subspace>
      <Socket>/tmp/subspace</Socket>
      <SlotSize>256</SlotSize>
      <Slots>132</Slots>
      <MaxActiveMessages>64</MaxActiveMessages>
      <ExpectedSubscribers>1</ExpectedSubscribers>
      <FixedSize>false</FixedSize>
      <Reliable>false</Reliable>
      <LogDroppedMessages>false</LogDroppedMessages>
      <DetectDroppedMessages>false</DetectDroppedMessages>
    </Subspace>
  </SharedMemory>
</Domain>
```

If `Slots` or `MaxActiveMessages` is `0`, CycloneDDS derives conservative
values from DDS history depth and `ExpectedSubscribers`. For explicit sizing,
`Slots` must include Subspace's publisher/subscriber bookkeeping headroom in
addition to active messages; `132` is the smallest tested slot count that cleanly
supports `MaxActiveMessages=64` for the single-subscriber local benchmark.
`Reliable=false` is the default until the direct backend's reliable-mode
interaction with DDS discovery and startup ordering has been tuned. The
benchmark configs set `DetectDroppedMessages=false` because DDS/performance_test
already counts lost samples from sample IDs. The
performance matrix runner writes per-scenario XML files so message size,
fan-out, and queue depth benchmarks exercise the intended Subspace sizing.
