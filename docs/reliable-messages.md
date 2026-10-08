# Reliable Messages

Subspace normally favors low latency over guaranteed delivery. An unreliable
publisher may reuse old slots, so a subscriber that falls behind can miss
messages. Reliable publishers and subscribers add backpressure instead: a
reliable publisher does not overwrite a message that a reliable subscriber
still needs.

The delivery guarantee applies between a publisher created with
`PublisherOptions::SetReliable(true)` and a subscriber created with
`SubscriberOptions::SetReliable(true)`. An unreliable subscriber does not
receive the same guarantee and does not require a reliable publisher to wait
until it has seen every message. Likewise, a reliable subscriber can still miss
messages from an unreliable publisher.

## Slot Lifetime

Messages are stored in a fixed number of shared-memory slots. Reliable delivery
uses each slot's delivery state and reference counts:

1. A publisher claims a free or retired slot and publishes a message in it.
2. Each matching subscriber is notified that the slot is available.
3. A reliable subscriber claims a reliable reference when it reads the
   message.
4. The slot cannot be reused while that reliable reference, or another active
   message reference, remains.
5. Once all required subscribers have seen the message and all references have
   been released, the slot retires and can be claimed by a publisher again.

The publisher considers slots in publication order. It will not skip past the
oldest message if doing so would allow a reliable subscriber to miss that
message. This preserves ordered, lossless delivery rather than converting
temporary congestion into a gap.

## Publisher Backpressure

`Publisher::GetMessageBuffer()` does not block automatically. When a reliable
publisher cannot safely claim a slot, it returns an OK `StatusOr` containing
`nullptr`. `GetMessageBufferSpan()` reports the same condition with an empty
span. This is temporary unavailability, not an error.

The publisher should wait for its poll file descriptor and then retry. The
`Publisher::Wait()` overloads provide blocking, timed, interruptible, and
coroutine-aware waits:

```cpp
for (;;) {
  auto buffer = publisher.GetMessageBuffer(message_size);
  if (!buffer.ok()) {
    return buffer.status();
  }
  if (*buffer == nullptr) {
    auto status = publisher.Wait();
    if (!status.ok()) {
      return status;
    }
    continue;
  }

  std::memcpy(*buffer, data, message_size);
  return publisher.PublishMessage(message_size).status();
}
```

A wake-up means that the publisher may try again; another publisher can claim
the newly available slot first, so the retry must still handle `nullptr`.
Publishing resumes when a subscriber advances, releases its references, or
disconnects and the server cleans up its slot ownership.

## Slow Subscribers

The slot count determines how far a reliable publisher can run ahead. A slow
reliable subscriber consumes that capacity until the channel fills. At that
point:

- reliable publishers stop obtaining buffers and wait;
- no reliable messages are dropped or overwritten;
- faster subscribers can drain their existing backlog but receive no newly
  published messages because the publishers cannot advance; and
- backpressure propagates to whichever component produces messages for those
  publishers.

Consequently, sustained throughput is limited by the slowest reliable
subscriber. More slots absorb short bursts but do not solve a subscriber that
is consistently slower than the publisher. Holding several `Message` objects
or `subspace::shared_ptr` values also holds several slots and can make the
publisher stall sooner. `max_active_messages` bounds how many active messages a
subscriber can hold.

Do not perform a blocking publisher wait on the only thread or coroutine that
can advance the slow subscriber. Use separate execution contexts or integrate
the publisher and subscriber file descriptors into the same event loop.

## The Subscriber's Active Message

A reliable subscriber always keeps an internal reference to its most recently
read active message, even when `keep_active_message` is false. This keeps the
shared-memory buffer valid and prevents its slot from being recycled while that
message is still the subscriber's current message.

The subscriber releases this internal reference when it advances to another
message, when `Subscriber::ClearActiveMessage()` is called, or when the
subscriber is destroyed. Any returned `Message`, copied `Message`, or
`subspace::shared_ptr` keeps its own reference; the slot remains active until
all such references are also released.

Calling `ClearActiveMessage()` explicitly tells Subspace that the subscriber no
longer needs its internally retained current message. The application must not
continue using the message's buffer unless it holds another owning message
reference. For unreliable subscribers, retaining the current message remains
opt-in through `SubscriberOptions::SetKeepActiveMessage(true)`.

This automatic retention is part of reliable backpressure. Without it, the
subscriber could release the slot as soon as a temporary return value was
destroyed, allowing the publisher to reuse the buffer while it was still the
subscriber's active message.

## Choosing Reliability

Use reliable delivery when every message must be processed and it is acceptable
for a slow consumer to throttle producers. Use unreliable delivery for
latest-state, telemetry, or high-rate streams where bounded latency is more
important than retaining every intermediate message.

Monitor processing latency and size the slot count for expected bursts. A
reliable subscriber should release owning message references promptly after
processing and should be isolated from work that can block indefinitely.
