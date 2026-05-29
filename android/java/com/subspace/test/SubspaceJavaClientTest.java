// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

package com.subspace.test;

import com.subspace.SubspaceClient;
import com.subspace.SubspaceException;
import com.subspace.SubspaceMessage;
import com.subspace.SubspacePublisher;
import com.subspace.SubspaceSubscriber;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class SubspaceJavaClientTest {
    private static final String WORK_DIR = getEnv("SUBSPACE_TEST_WORK_DIR", "/data/local/tmp");
    private static final String SERVER = getEnv("SUBSPACE_SERVER", WORK_DIR + "/subspace_server");
    private static final String SOCKET = getEnv("SUBSPACE_SOCKET", WORK_DIR + "/subspace");
    private static final String NATIVE_LIB_DIR =
            getEnv("SUBSPACE_NATIVE_LIB_DIR", WORK_DIR + "/android_libs");
    private static final String LIB_PATH =
            getEnv("SUBSPACE_LD_LIBRARY_PATH", NATIVE_LIB_DIR + ":" + WORK_DIR);
    private static final long SERVER_TIMEOUT_MS = 10_000;
    private static final long MESSAGE_TIMEOUT_MS = 5_000;

    private SubspaceJavaClientTest() {}

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    public static void main(String[] args) throws Exception {
        Process server = null;
        try {
            preloadNativeLibraries();
            deleteSocket();
            server = startServer();
            waitForServer();

            testPublishSubscribe();
            testCancelPublish();

            System.out.println("SubspaceJavaClientTest passed");
        } finally {
            stopServer(server);
            deleteSocket();
        }
    }

    private static void preloadNativeLibraries() {
        loadIfPresent("libc++.so");
        loadIfPresent("libprotobuf-cpp-full.so");
        loadIfPresent("libsubspace_client.so");
        loadIfPresent("libsubspace_jni.so");
    }

    private static void loadIfPresent(String libraryName) {
        File library = new File(NATIVE_LIB_DIR, libraryName);
        if (library.exists()) {
            System.load(library.getAbsolutePath());
        }
    }

    private static Process startServer() throws IOException {
        ProcessBuilder builder = new ProcessBuilder(SERVER);
        builder.directory(new File(WORK_DIR));
        builder.redirectErrorStream(true);
        Map<String, String> env = builder.environment();
        env.put("LD_LIBRARY_PATH", LIB_PATH);

        Process process = builder.start();
        Thread outputThread = new Thread(() -> copyServerOutput(process), "subspace-server-output");
        outputThread.setDaemon(true);
        outputThread.start();
        return process;
    }

    private static void copyServerOutput(Process process) {
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("[subspace_server] " + line);
            }
        } catch (IOException ignored) {
        }
    }

    private static void waitForServer() throws Exception {
        long deadline = System.currentTimeMillis() + SERVER_TIMEOUT_MS;
        SubspaceException lastError = null;
        while (System.currentTimeMillis() < deadline) {
            try (SubspaceClient ignored = new SubspaceClient(SOCKET, "java-wait")) {
                return;
            } catch (SubspaceException e) {
                lastError = e;
                Thread.sleep(100);
            }
        }
        throw new AssertionError("Timed out waiting for subspace_server", lastError);
    }

    private static void testPublishSubscribe() throws Exception {
        byte[] payload = "hello from java".getBytes(StandardCharsets.UTF_8);
        try (SubspaceClient client = new SubspaceClient(SOCKET, "java-test");
             SubspacePublisher publisher =
                     client.createPublisher("/java/test/pubsub", 128, 4);
             SubspaceSubscriber subscriber =
                     client.createSubscriber("/java/test/pubsub")) {

            ByteBuffer buffer = publisher.getMessageBuffer(payload.length);
            assertTrue(buffer != null, "publisher returned null message buffer");
            buffer.put(payload);
            long ordinal = publisher.publishMessage(payload.length);
            assertTrue(ordinal >= 0, "publish returned negative ordinal");

            SubspaceMessage message = readMessageEventually(subscriber);
            assertEquals(payload.length, message.getLength(), "message length");

            byte[] actual = new byte[message.getLength()];
            message.getData().get(actual);
            assertTrue(Arrays.equals(payload, actual),
                    "payload mismatch: expected '" + new String(payload, StandardCharsets.UTF_8)
                            + "' got '" + new String(actual, StandardCharsets.UTF_8) + "'");
        }
    }

    private static void testCancelPublish() {
        try (SubspaceClient client = new SubspaceClient(SOCKET, "java-cancel");
             SubspacePublisher publisher =
                     client.createPublisher("/java/test/cancel", 64, 2)) {
            ByteBuffer buffer = publisher.getMessageBuffer(16);
            assertTrue(buffer != null, "publisher returned null buffer for cancel test");
            buffer.put("cancelled".getBytes(StandardCharsets.UTF_8));
            publisher.cancelPublish();
        }
    }

    private static SubspaceMessage readMessageEventually(SubspaceSubscriber subscriber)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + MESSAGE_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            SubspaceMessage message = subscriber.readMessage();
            if (message != null) {
                return message;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Timed out waiting for published message");
    }

    private static void stopServer(Process server) {
        if (server == null) {
            return;
        }
        server.destroy();
        try {
            if (!server.waitFor(2, TimeUnit.SECONDS)) {
                server.destroyForcibly();
                server.waitFor(2, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.destroyForcibly();
        }
    }

    private static void deleteSocket() {
        File socket = new File(SOCKET);
        if (socket.exists() && !socket.delete()) {
            throw new AssertionError("Failed to delete stale socket " + SOCKET);
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void assertEquals(int expected, int actual, String field) {
        if (expected != actual) {
            throw new AssertionError(field + ": expected " + expected + " got " + actual);
        }
    }
}
