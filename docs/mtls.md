# Mutual TLS

Bridge connections and tunnel connections can require a certificate from both
sides. The server uses BoringSSL when built with Bazel and OpenSSL when built
with CMake. It does not implement any cryptography itself.

The build leaves mTLS out unless you ask for it. A server built without it
keeps the cleartext protocols and refuses `--mtls_cert`.

## Building

Bazel, with BoringSSL:

```bash
bazelisk build --config=mtls //server:subspace_server
```

`--config=mtls` is `--//:mtls=true`. A downstream module selects
`@subspace//:mtls`.

CMake, with the system OpenSSL:

```bash
cmake -S . -B build -DSUBSPACE_ENABLE_MTLS=ON
cmake --build build --target subspace_server
```

Android Soong builds leave it off. Add `-DSUBSPACE_ENABLE_MTLS=1` to
`subspace_defaults` and link `libcrypto` and `libssl` to turn it on.

## Running

All three files are PEM. The private key is unencrypted. Every peer
certificate must be signed by the CA. The server does not check hostnames:
native bridge peers are discovered by address, and tunnel peers are named in
the tunnel configuration.

```bash
subspace_server \
  --mtls_cert=/etc/subspace/server.pem \
  --mtls_key=/etc/subspace/server.key \
  --mtls_ca=/etc/subspace/ca.pem
```

A program that embeds the server calls `Server::SetMtlsCredentials` before
`Server::Run`.

When the files are set, both the native bridge and the tunnel speak TLS on
every connection they open or accept. That includes bridge retirement
sockets and tunnel discovery, which uses the same TCP port as tunnel data.
UDP discovery and `--tcp_discovery` stay in cleartext. A peer that does not
present a certificate the CA signed is disconnected, so a tunnel built this
way no longer speaks the cleartext cruise tunnel protocol.

The handshake is TLS 1.2 or 1.3. Session tickets are off, so a tunnel
receiver does not have to send application data.
