// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

package com.subspace;

/** Thrown when a Subspace native operation fails. */
public class SubspaceException extends RuntimeException {
    public SubspaceException(String message) {
        super(message);
    }

    public SubspaceException(String message, Throwable cause) {
        super(message, cause);
    }
}
