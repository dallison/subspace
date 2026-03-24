// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

package com.subspace;

import java.nio.ByteBuffer;

/**
 * A message received from a Subspace channel.
 *
 * <p>The {@link #getData()} buffer is a direct {@link ByteBuffer} pointing
 * into shared memory.  It is only valid until the next call to
 * {@link SubspaceSubscriber#readMessage}.
 */
public class SubspaceMessage {
    private final ByteBuffer data;
    private final long timestamp;
    private final long ordinal;
    private final int slotId;
    private final int length;

    SubspaceMessage(ByteBuffer data, long timestamp, long ordinal,
                    int slotId, int length) {
        this.data = data;
        this.timestamp = timestamp;
        this.ordinal = ordinal;
        this.slotId = slotId;
        this.length = length;
    }

    /**
     * Direct ByteBuffer containing the message payload.  Points into shared
     * memory; valid until the next read.
     */
    public ByteBuffer getData() {
        return data;
    }

    /** Nanosecond timestamp when the message was published. */
    public long getTimestamp() {
        return timestamp;
    }

    /** Monotonically increasing sequence number within the channel. */
    public long getOrdinal() {
        return ordinal;
    }

    /** Internal slot identifier. */
    public int getSlotId() {
        return slotId;
    }

    /** Size of the message payload in bytes. */
    public int getLength() {
        return length;
    }
}
