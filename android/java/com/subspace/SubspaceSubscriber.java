// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

package com.subspace;

/**
 * Subscribes to messages from a Subspace channel via shared memory.
 *
 * <p>Messages are returned as {@link SubspaceMessage} objects containing a
 * direct {@link java.nio.ByteBuffer} that points into shared memory (zero
 * copy).
 *
 * <pre>{@code
 * SubspaceMessage msg = subscriber.readMessage(false);
 * if (msg != null) {
 *     ByteBuffer data = msg.getData();
 *     // process data...
 * }
 * }</pre>
 */
public class SubspaceSubscriber implements AutoCloseable {
    private long nativeHandle;

    SubspaceSubscriber(long handle) {
        this.nativeHandle = handle;
    }

    /**
     * Read the next available message.
     *
     * @param newest if {@code true}, skip to the most recent message;
     *               if {@code false}, read the next message in sequence
     * @return a {@link SubspaceMessage}, or {@code null} if no message is
     *         available
     * @throws SubspaceException on failure
     */
    public SubspaceMessage readMessage(boolean newest) {
        checkOpen();
        return nativeReadMessage(nativeHandle, newest);
    }

    /** Read the next message in sequence. */
    public SubspaceMessage readMessage() {
        return readMessage(false);
    }

    /**
     * Return the raw file descriptor that can be polled for readability
     * (triggered when new messages are available).
     */
    public int getPollFd() {
        checkOpen();
        return nativeGetPollFd(nativeHandle);
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
            throw new IllegalStateException("SubspaceSubscriber is closed");
        }
    }

    // Native methods
    private static native SubspaceMessage nativeReadMessage(long handle,
                                                            boolean newest);
    private static native int nativeGetPollFd(long handle);
    private static native String nativeGetName(long handle);
    private static native void nativeDestroy(long handle);
}
