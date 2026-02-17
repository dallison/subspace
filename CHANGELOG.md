# CHANGELOG.md

## Subspace Version 2.2.0

### Client API Improvements

#### Publisher Options
- **Retirement notification support**: Enhanced publisher options with retirement notification capabilities
  - `notify_retirement` option for publishers to receive slot retirement notifications
  - `GetRetirementFd()` method to obtain file descriptor for retirement notifications
  - Improved retirement tracking for GPU memory management and external resource cleanup

#### Thread Safety Features
- **Configurable thread safety**: New thread safety options for concurrent client usage
  - Optional mutex protection for all client operations
  - Automatic message buffer locking during publish operations
  - Configurable locking behavior for performance optimization
  - Thread-safe message retirement handling

#### Checksums
- **Configurable checksum generation and checking**: Optionally allows a CRC32 checksum to be added into the `MessagePrefix` for a published message and checked in subscribers.  A message with a failed checksum can either be dropped by the client or received with a failure notification.
  
#### Portability
- **POSIX shared memory**: for non-Linux systems, uses POSIX shared memory with a shadow file in /tmp.

#### Bridge Support
- **Enhanced bridge publisher/subscriber options**: Improved bridge functionality in client options
  - Better bridge detection and configuration
  - Enhanced virtual channel support for bridged communications
  - Improved multiplexer channel handling across bridges

### New Features

#### Server Plugin Architecture
- **Dynamic plugin loading**: Added comprehensive plugin system for extending server functionality
  - Plugin interface with lifecycle hooks: `onStartup`, `onReady`, `onShutdown`
  - Event-driven callbacks for channel and user management: `onNewChannel`, `onRemoveChannel`, `onNewPublisher`, `onRemovePublisher`, `onNewSubscriber`, `onRemoveSubscriber`
  - Plugin context system with integrated logging support
  - Single-threaded coroutine-based execution model for plugins
  - Dynamic library loading support with C-style function interfaces

#### Client-Side Thread Safety
- **Optional thread safety support**: Added thread-safe client operations for multi-threaded applications
  - `SetThreadSafe(true)` method to enable mutex-protected client operations
  - Thread-safe publisher message buffer management with automatic locking
  - Configurable locking behavior with option to disable automatic locking for zero-copy operations
  - Mutual exclusion protection for concurrent client operations
  - Thread-safe retirement notification handling across multiple threads

#### Message Checksum Support
- **Hardware-accelerated checksums**: Integrated CRC32 checksum calculation for message integrity
  - Hardware CRC32 instruction support with fallback implementations
  - `CalculateChecksum()` and `VerifyChecksum()` functions for multi-span data validation
  - ARM assembly optimizations for CRC32 calculations
  - Configurable hardware acceleration via `SUBSPACE_HARDWARE_CRC` compilation flag
  - Template-based checksum calculation for arbitrary data spans

### Testing Infrastructure

#### Bridge Testing Framework
- **Comprehensive bridge retirement testing**: Added extensive test coverage for message retirement across bridges
  - `BasicRetirement` test: Validates basic retirement notification functionality
  - `MultipleRetirement` test: Tests retirement notifications for multiple messages with slot tracking
  - `MultipleRetirement2` test: Alternative retirement scenario testing with different slot allocation patterns
- **Multi-server bridge testing**: Enhanced test infrastructure supporting multiple Subspace servers
  - Two-server test setup with proper lifecycle management
  - Bridge notification pipes for inter-server communication testing
  - Retirement notification validation across server boundaries

#### Multi-threaded Testing
- **Concurrent operation validation**: Extensive multi-threaded testing infrastructure
  - Stress tests with multiple concurrent clients and channels
  - Latency testing under multi-threaded conditions
  - Thread safety validation for concurrent publisher/subscriber operations
  - Performance benchmarking for threaded vs non-threaded scenarios

#### Test Utilities
- **Improved test helpers**: Enhanced utility functions for bridge testing
  - `WaitForSubscribedMessage`: Robust waiting mechanism for bridge subscription notifications
  - Better signal handling for test debugging (`SigQuitHandler`)
  - Enhanced coroutine debugging capabilities for multi-server scenarios

### Server Infrastructure

#### Bridge Communication
- **Enhanced bridge transmitter functionality**: Improved the bridge transmitter coroutine system
  - Better retirement notification handling across bridges
  - Improved error handling for bridge connection failures
  - Enhanced retirement socket management

#### Retirement Notification System
- **Robust retirement tracking**: Improved retirement notification system for bridged channels
  - Better tracking of active messages across bridge connections
  - Enhanced retirement receiver coroutine functionality
  - Improved slot retirement correlation across server boundaries

#### Plugin Integration
- **Server extension points**: Comprehensive plugin integration throughout server lifecycle
  - Plugin initialization during server startup
  - Event notifications for all channel and user lifecycle events
  - Graceful plugin shutdown handling
  - Error handling and logging for plugin operations


### Documentation

#### Version 2 Features
- **Comprehensive feature documentation**: Enhanced documentation of Subspace Version 2 features
  - Lock-free shared memory implementation details
  - Message retirement notification system documentation
  - Multiplexed virtual channels usage patterns
  - C client API documentation
  - Thread safety features and guidelines
  - Server plugin development guide


### Bug Fixes

#### Bridge Test Improvements
- **Fixed bridge test debug output**: Removed excessive debug logging in `bridge_test.cc` that was cluttering test output
  - Removed verbose bridge notification length logging in `WaitForSubscribedMessage` function
  - Improved test output clarity for bridge functionality testing

#### Bridge Notification System
- **Enhanced bridge notification handling**: Improved the robustness of bridge notification processing
  - Better error handling for bridge notification pipe operations
  - Cleaner separation of bridge notification logic from debug output

### Infrastructure Improvements

#### Build System
- **Enhanced CMake support**: Improved CMake build configuration
  - Better dependency management for server components
  - Enhanced library linking for cross-platform builds
  - Improved target configuration for test executables
  - Plugin compilation support

#### Development Tools
- **Better debugging support**: Enhanced debugging capabilities for multi-server scenarios
  - Improved coroutine introspection tools
  - Better signal handling for development debugging
  - Enhanced test output formatting
  - Plugin debugging and logging infrastructure

### Performance Enhancements

#### Hardware Acceleration
- **Optimized checksum calculations**: Hardware-accelerated CRC32 for improved performance
  - ARM assembly optimizations for mobile and embedded platforms
  - Intel hardware CRC32 instruction utilization
  - Fallback implementations for compatibility

#### Thread Safety Optimization
- **Selective locking**: Configurable thread safety to minimize performance impact
  - Optional locking for zero-copy operations
  - Minimal mutex contention in multi-threaded scenarios
  - Atomic operations for retirement trigger management

### Breaking Changes
- None in this release - all changes are backward compatible bug fixes and improvements

### Migration Notes
- No migration required for existing applications
- New retirement notification features are opt-in via publisher options
- Thread safety must be explicitly enabled via `SetThreadSafe(true)`
- Server plugins require separate compilation and loading
- Enhanced bridge testing infrastructure is available for developers working with multi-server deployments

---

**Note**: This release focuses primarily on improving the stability and testability of the bridge communication system, while adding significant new capabilities for server extensibility, client thread safety, and message integrity validation. All new features maintain backward compatibility with existing Subspace applications.