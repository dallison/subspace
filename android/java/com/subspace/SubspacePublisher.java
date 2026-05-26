// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

package com.subspace;

import java.nio.ByteBuffer;

/**
 * Publishes messages to a Subspace channel via shared memory.
 *
 * <p>Typical usage:
 * <pre>{@code
 * ByteBuffer buf = publisher.getMessageBuffer(128);
 * buf.put(myData);
 * publisher.publishMessage(myData.length);
 * }</pre>
 *
 * <p>The {@link ByteBuffer} returned by {@link #getMessageBuffer} is a direct
 * buffer that points into the shared-memory region.  Write your payload into it
 * and then call {@link #publishMessage} with the actual size.  If you decide
 * not to publish, call {@link #cancelPublish} instead.
 */
public class SubspacePublisher implements AutoCloseable {
    private long nativeHandle;

    SubspacePublisher(long handle) {
        this.nativeHandle = handle;
    }

    /**
     * Obtain a direct {@link ByteBuffer} backed by the next available message
     * slot.  If the slot is smaller than {@code maxSize}, the buffer will be
     * resized.
     *
     * @param maxSize requested minimum buffer size; pass -1 to use the current
     *                slot size
     * @return a direct ByteBuffer, or {@code null} if no slot is available
     *         (reliable publisher with all slots in use)
     * @throws SubspaceException on failure
     */
    public ByteBuffer getMessageBuffer(int maxSize) {
        checkOpen();
        return nativeGetMessageBuffer(nativeHandle, maxSize);
    }

    /** Get a message buffer using the default slot size. */
    public ByteBuffer getMessageBuffer() {
        return getMessageBuffer(-1);
    }

    /**
     * Publish the message previously obtained via {@link #getMessageBuffer}.
     *
     * @param messageSize number of bytes written into the buffer
     * @return the ordinal of the published message
     * @throws SubspaceException on failure
     */
    public long publishMessage(long messageSize) {
        checkOpen();
        return nativePublishMessage(nativeHandle, messageSize);
    }

    /**
     * Cancel a pending publish.  Call this instead of {@link #publishMessage}
     * if you obtained a buffer but decide not to send it.
     */
    public void cancelPublish() {
        checkOpen();
        nativeCancelPublish(nativeHandle);
    }

    /**
     * Return the raw file descriptor that can be polled for writability
     * (reliable publisher: triggered when a slot becomes free).
     */
    public int getPollFd() {
        checkOpen();
        return nativeGetPollFd(nativeHandle);
    }

    /** Current slot size in bytes. */
    public int getSlotSize() {
        checkOpen();
        return nativeGetSlotSize(nativeHandle);
    }

    /** Channel name. */
    public String getName() {
        checkOpen();
        return nativeGetName(nativeHandle);
    }

    @Override
    public void close() {
        if (nativeHandle != 0) {
            nativeDestroy(nativeHandle);
            nativeHandle = 0;
        }
    }

    private void checkOpen() {
        if (nativeHandle == 0) {
            throw new IllegalStateException("SubspacePublisher is closed");
        }
    }

    // Native methods
    private static native ByteBuffer nativeGetMessageBuffer(long handle,
                                                            int maxSize);
    private static native long nativePublishMessage(long handle,
                                                    long messageSize);
    private static native void nativeCancelPublish(long handle);
    private static native int nativeGetPollFd(long handle);
    private static native int nativeGetSlotSize(long handle);
    private static native String nativeGetName(long handle);
    private static native void nativeDestroy(long handle);
}
