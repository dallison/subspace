# New features in Subspace version 2

This version of Subspace contains a couple of new features that enhance the stability and usability.

## Lock-free shared memory
The initial version of Subspace used a single shared lock in every channel's control block.  This lock was
acquired by all publishers and subscibers when accessing the channel to publish and read messages.

The use of a lock is performant and usually safe, but under certain circumstances can cause issues:

1. If a process exits while the publisher or subscriber holds the lock, nothing else can ever acquire the lock, thus rendering the channel unusable
2. Every lock acquisistion is an opportunity for the kernel to perform a context switch.  Although a context switch can occur at any time, a lock is a good hint to the kernel and it might take the opportunity, especially if the lock is contended.  A context switch can delay the publishing of a message and this might not be desired
3. Locks can be slow to acquire and release
4. Under contention conditions, locks can cause delays in publishing.

Although these possibilities are remote (especially #1), they *could* happen so the removal of the lock makes for more robust software.

The lock-free implementation removes the shared memory lock and swaps a simple linked-list algorithm while holding the lock for more complex searches, sorts and compare-swap operations that operate on the shared memory directly.  Writing lock-free code is very difficult and inherently racy so this is a major undertaking.  It might also be a little slower but that's probably in the noise floor.

### How it works
The locked version of subspace organized the slots in the channel into 3 linked lists and the algorithm simply swapped slots between these lists by manipulating the `previous` and `next` offsets while holding the lock.

The lock-free version is more complex.  It relies on a 64-bit atomic integer called `refs` in the message slot that contains either the reference counters for how many subscribers have a reference to the slot, or a special value specifying that a publisher owns the slot.  Instead of linked lists, each subscriber holds an `atomic bitset` that specifies what slots hold messages that the subscriber has not yet seen.

A publisher gets a message slot by looking for a slot in the channel that has the earlist timestamp (or no timestamp) and no subscriber references.  When it finds such a slot, it will attempt to atomically compare-and-swap the `refs` with its own publisher ID value.  If this compare-and-swap fails, something else (another publisher or subscriber) has grabbed the slot and the publisher just tries again.

When a publisher publishes a message, it will set the timestamp and ordinal for the message and set the `refs` field in the slot to 0.  It also sets the bit in every subscriber's bitset telling the subscriber that the slot ID contains a message.
Once the `refs` field is zero, a subscriber or another publisher will be able to see the slot and when this happens is not controlled.

On the subscriber side, each subscriber will traverse its bitset and collect the slots that potentially contain a message it has not yet seen.  It will then sort the slots by timestamp and choose the slot with the earlies timestamp.  Once it has chosen a slot, it will attempt to atomically increment `refs` field in the slot to signify that it has a reference to the slot.  This is done using a `compare-and-swap` operation and if that fails, it will go back and look for another slot.

A subscriber can only have one reference to a particualar slot, but it may hold references to many slots.  The `Message` object returned from a read is a smart pointer that holds a reference to the slot while it is alive.  When it goes out of scope, the slot's reference from that subscriber is removed.

## Change to Message objects
The object that holds information about received messages is still called `Message` but is is now implemented as a `std::shared_ptr<ActiveMessage>` instead of a simple struct.  An `ActiveMessage` is a smart message reference that releases the message's slot when it goes out of scope.  You can't copy an `ActiveMessage` but you can move it.  A `Message` is a copyable object that wraps an `ActiveMessage` so you can pass those around like any other struct.

This change is due to the lock-free implementation that only allows a subscriber to hold one reference to a slot (but it can hold references to more than one slot).

When all `Message` objects go out of scope, the `ActiveMessage` will be destructed and the message slot's reference will be removed from the subscriber.

## Renaming of `max_shared_ptrs` subscriber option.

A consequence of the change to `ActiveMessage` is that the `max_shared_ptrs` option has been renamed as `max_active_messages` and there needs to be at least 1 of them for a subscriber.

## Changed implemention of `shared_ptr` and `weak_ptr`
The previous version of Subpace used a custom implementation of a `shared_ptr` and `weak_ptr` that referred to slots and mimicked the style and behavior of the standard library objects of the same name.

This version, due to the complexity of implementing a lock-free algorithm, changes the implementation to wrap the standard library implementation and is thus much simpler.  It should work in the same way so you shouldn't see any difference if you used the old versions.

## Message callbacks.
You can now register a callback function for a subsciber that can be invoked for every message received by the subscriber.  The function is called from an invocation of `ProcessAllMessages` on the same subscriber.  This will call the registered callback function for every available message, returning when there are no more messages available.  The callback function is called with a pointer to `Subscriber` and the instance of the `Message` received.  

An example of the use of this is:
```c++
auto status = sub->RegisterMessageCallback([](Subscriber* sub, Message m) {
  // Handle message m.
});

for (;;) {
  status = sub->Wait();
  status = sub->ProcessesAllMessages();
}

```

## Multiplexed virtual channels
There is a style of IPC usage that insists on only one publisher per channel.  If you are a proponent of this, you will probably be creating a bunch of channels, perhaps a set of them per process/node that all have one publisher and a few subscribers.  In an IPC system that uses TCP for transport this isn't a big deal since they will use the kernel's pre-allocated TCP buffers for their message storage, but in a shared memory IPC system, each channel will have a ring buffer of shared memory allocated for them.

As the system grows, the number of channels also grows and the amount of shared memory increases accordingly.  This is a bit of stress on the memory.

The new feature in this release is the ability to create a set of channels that all share the same `multiplexer` channel.  These are called `virtual channels` and all publishers and subscribers work with them in exactly the same manner as with normal channels. 

To create a virtual channel set the `mux` member of the publisher or subscriber options to the name of another channel that will be used as the multiplexer channel.  All virtual channels that use the same multiplexer channel will shared the slots in that multiplexer channel instead of allocating their own shared memory.  Like any other channel, a multiplexer channel will be created by the first publisher or subscriber to use it.  You cannot create a publisher to a multiplexer channel that is not a virtual publisher, but you can create a subscriber to one.

Each virtual channel has a `vchan_id` (virtual channel ID) allocated for it.  This is an integer value that can be either specified by the creator of the publisher or subscriber or may be allocated by the server.  To specify your own virtual channel ids, use the `vchan_id` option in the publisher or subscriber options.  If you omit this option, the server will allocate one for you.  There is also a limit of the range of virtual channel ID numbers you can use, ranging from 0 to `subspace::kMaxVchanId - 1`.

A subscriber to virtual channel will read a message that contains the `vchan_id` of the virtual channel to which the message was published.  It is up to the receiver to determine what channel name this corresponds to.  If you are assigning the virtual channel IDs yourself, you will know this mapping.  You will really only need to know this if you are subscribing to the multiplexer channel rather than individual virtual channel subscribers.

Since all the publishers and subscribers to the multiplexer channel share the multiplexer's slots, you will probably need more slots in the multiplexer channel to avoid dropping messages.  Like all channels, the publisher will always choose the oldest slot when dropping messages, regardless of the virtual channel ID.  This means that faster publishers cause slower subscribers to miss messages on their channels.  This is a trade-off, you can either allocate a lot of channels, each with their own ring buffer, or you can share a ring buffer.  The latter will use less memory.

But there is another feature you can use to reduce the number of dropped messages on multiplexer channels.  You can create a subscriber to the multiplexer itself which will see all messages on the channel, regardless of the virtual channel ID.  Each message returned will contain the `vchan_id` of the sender, and since you can control the virtual channel IDs, you can translate the `vchan_id` into a channel name if you need to.

### Multplexed virtual publisher
To create multiplexed virtual publisher use something akin to the following code.  Every virtual publisher created on the same multiplexer must use the same slot-size and num-slots values.  You will probably need to allocate more slots that you would normally do, depending on the number of virtual channels that will share the multiplexer.

To create a multiplexed virtual publisher that has an `vchan_id` allocated by the server.

```c++
auto pub = client.CreatePublisher("/log/foo", 4096, 1000, {.mux = "/logs_mux"});

```

You can call `pub->GetVirtualChannelId()` to see what `vchan_id` was assigned by the server.

Alternatively, if you need to control the `vchan_ids` yourself you can use:

```c++
auto pub = client.CreatePublisher("/log/foo", 4096, 1000, {.mux = "/logs_mux", .vchan_id = 32});

```

The virtual channel ID must be greater or equal to zero and less than `subspace::kMaxVchanId`.

### Multiplexed virtual subscriber
A multliplex virtual subscriber is almost the same as a non-multiplexed one:

```c++
auto sub = client.CreateSubscriber("/log/foo", {.mux = "/logs_mux"});
```

Like the publisher, you can call `sub->GetVirtualChannelId()` to get the `vchan_id` assigned by the server or you can assign it yourself, in which case it is up to you to ensure it is unique and in range.

When you call `Wait()` or `ReadMessage()` on the subscriber, it will behave like a regular non-multiplexed subscriber and you will only see the message that use the same `vchan_id`.

### Multiplexer channel subscriber
As an alternative to creating individual subscribers for virtual channels, you can create a subscriber that sees all messages published to the multiplexer channel itself, regardless of the `vchan_id` used by the publisher.

```c++
auto sub = client.CreateSubscriber("/logs_mux");

```
Notice that the subscriber is created on the multiplexer channel name and does not use the `mux` option.  This subscriber will be able to read all messages sent on the multiplexer and each message contains a `vchan_id` member that specifies that virtual channel the message is carried on.  You can use this number to determine the name of the virtual channel either by recording the values assigned by the server or using the mapping you chose to determine `vchan_ids`.

If you are using a multi-computer system where messages are sent over a network between computers using a multiplexer subscriber (the most efficient way), it is probably best to determine the virtual channel ID vs channel name mapping ahead of time and apply the same mapping to all computers. 

## Client side buffer allocations
In version 1 the buffers for channels were allocated by the server and communicated to the client via
a file descriptor.  This had the disadvantage that a resize operation was a round-trip to the server
allowing the system to perform context switches and reducing performance.

In version 2, the allocation of buffers is on the client and uses the file system as the namespace.
On Linux it uses files in `/dev/shm` and on MacOs, the files are in `/tmp` and those map to shared
memory segments held somewhere else not visible to the file system.

This speeds up resize operations substantially and are now around 10 microseconds on a fast
computer.  Also the variation in resize speed is greatly reduced.

## C Client
A new C-language client has been added.  This provides most of the functionality present
in the C++ client with many fewer dependencies.  Being in C, it's more portable too.  It just
maps onto the C++ client but provides C-linkage functions.

This should be easier to map into other languages like Rust or Go in the future.
