// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

package com.subspace;

/**
 * Java wrapper for the Subspace C++ client library.
 *
 * <p>Connects to a running Subspace server and creates publishers/subscribers
 * for inter-process shared-memory communication.
 *
 * <pre>{@code
 * try (SubspaceClient client = new SubspaceClient("/data/local/tmp/subspace", "myapp")) {
 *     SubspacePublisher pub = client.createPublisher("/sensor", 1024, 8);
 *     SubspaceSubscriber sub = client.createSubscriber("/sensor");
 * }
 * }</pre>
 */
public class SubspaceClient implements AutoCloseable {
    static {
        System.loadLibrary("subspace_jni");
    }

    private long nativeHandle;

    /**
     * Connect to the Subspace server.
     *
     * @param socketPath Unix domain socket path of the server
     * @param clientName human-readable name for this client instance
     * @throws SubspaceException if the connection fails
     */
    public SubspaceClient(String socketPath, String clientName) {
        nativeHandle = nativeCreate(socketPath, clientName);
    }

    /** Connect to the server on the default Android socket path. */
    public SubspaceClient(String clientName) {
        this("/data/local/tmp/subspace", clientName);
    }

    /**
     * Create a publisher on the given channel.
     *
     * @param channelName channel name (e.g. "/sensor/imu")
     * @param slotSize    size of each message slot in bytes
     * @param numSlots    number of slots in the ring buffer
     * @param reliable    if true, publisher blocks when all slots are in use
     * @return a new publisher
     * @throws SubspaceException on failure
     */
    public SubspacePublisher createPublisher(String channelName, int slotSize,
                                             int numSlots, boolean reliable) {
        checkOpen();
        long pubHandle = nativeCreatePublisher(nativeHandle, channelName,
                                               slotSize, numSlots, reliable);
        return new SubspacePublisher(pubHandle);
    }

    /** Create a non-reliable publisher with default options. */
    public SubspacePublisher createPublisher(String channelName, int slotSize,
                                             int numSlots) {
        return createPublisher(channelName, slotSize, numSlots, false);
    }

    /**
     * Create a subscriber on the given channel.
     *
     * @param channelName channel name
     * @return a new subscriber
     * @throws SubspaceException on failure
     */
    public SubspaceSubscriber createSubscriber(String channelName) {
        checkOpen();
        long subHandle = nativeCreateSubscriber(nativeHandle, channelName);
        return new SubspaceSubscriber(subHandle);
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
            throw new IllegalStateException("SubspaceClient is closed");
        }
    }

    // Native methods
    private static native long nativeCreate(String socketPath,
                                            String clientName);
    private static native void nativeDestroy(long handle);
    private static native long nativeCreatePublisher(long handle,
                                                     String channelName,
                                                     int slotSize,
                                                     int numSlots,
                                                     boolean reliable);
    private static native long nativeCreateSubscriber(long handle,
                                                      String channelName);
}
