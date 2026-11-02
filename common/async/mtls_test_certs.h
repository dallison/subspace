// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

// Test-only certificate minting.  Included only from tests built with mTLS.

#ifndef _COMMON_ASYNC_MTLS_TEST_CERTS_H
#define _COMMON_ASYNC_MTLS_TEST_CERTS_H

#include "absl/status/statusor.h"
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <cstdio>
#include <ctime>
#include <memory>
#include <string>
#include <unistd.h>

namespace subspace::async {
namespace {

struct OpenSslFree {
  void operator()(EVP_PKEY *p) const { EVP_PKEY_free(p); }
  void operator()(EVP_PKEY_CTX *p) const { EVP_PKEY_CTX_free(p); }
  void operator()(X509 *p) const { X509_free(p); }
  void operator()(ASN1_TIME *p) const { ASN1_TIME_free(p); }
};

using KeyPtr = std::unique_ptr<EVP_PKEY, OpenSslFree>;
using CertPtr = std::unique_ptr<X509, OpenSslFree>;

inline absl::StatusOr<KeyPtr> GenerateKey() {
  std::unique_ptr<EVP_PKEY_CTX, OpenSslFree> ctx(
      EVP_PKEY_CTX_new_id(EVP_PKEY_EC, nullptr));
  if (ctx == nullptr || EVP_PKEY_keygen_init(ctx.get()) != 1 ||
      EVP_PKEY_CTX_set_ec_paramgen_curve_nid(ctx.get(),
                                             NID_X9_62_prime256v1) != 1) {
    return absl::InternalError("failed to initialize a test key");
  }
  EVP_PKEY *raw = nullptr;
  if (EVP_PKEY_keygen(ctx.get(), &raw) != 1 || raw == nullptr) {
    return absl::InternalError("failed to generate a test key");
  }
  return KeyPtr(raw);
}

inline bool AddBasicConstraints(X509 *cert, bool is_ca) {
  BASIC_CONSTRAINTS *constraints = BASIC_CONSTRAINTS_new();
  if (constraints == nullptr) {
    return false;
  }
  constraints->ca = is_ca ? 0xff : 0;
  X509_EXTENSION *ext =
      X509V3_EXT_i2d(NID_basic_constraints, /*crit=*/1, constraints);
  BASIC_CONSTRAINTS_free(constraints);
  if (ext == nullptr) {
    return false;
  }
  int ok = X509_add_ext(cert, ext, -1);
  X509_EXTENSION_free(ext);
  return ok == 1;
}

inline bool SetLifetime(X509 *cert) {
  const time_t now = time(nullptr);
  std::unique_ptr<ASN1_TIME, OpenSslFree> not_before(ASN1_TIME_new());
  std::unique_ptr<ASN1_TIME, OpenSslFree> not_after(ASN1_TIME_new());
  if (not_before == nullptr || not_after == nullptr ||
      ASN1_TIME_set(not_before.get(), now - 24 * 60 * 60) == nullptr ||
      ASN1_TIME_set(not_after.get(), now + 7 * 24 * 60 * 60) == nullptr) {
    return false;
  }
  return X509_set1_notBefore(cert, not_before.get()) == 1 &&
         X509_set1_notAfter(cert, not_after.get()) == 1;
}

inline absl::Status WriteCert(const std::string &path, X509 *cert) {
  FILE *file = fopen(path.c_str(), "w");
  if (file == nullptr || PEM_write_X509(file, cert) != 1) {
    if (file != nullptr) {
      fclose(file);
    }
    return absl::InternalError("failed to write " + path);
  }
  fclose(file);
  return absl::OkStatus();
}

inline absl::Status WriteKey(const std::string &path, EVP_PKEY *key) {
  FILE *file = fopen(path.c_str(), "w");
  if (file == nullptr ||
      PEM_write_PrivateKey(file, key, nullptr, nullptr, 0, nullptr, nullptr) !=
          1) {
    if (file != nullptr) {
      fclose(file);
    }
    return absl::InternalError("failed to write " + path);
  }
  fclose(file);
  return absl::OkStatus();
}

// A CA and two identities it signed, plus a second CA and one identity that
// the first CA will not accept.
struct TestCerts {
  std::string directory;
  std::string ca;
  std::string alpha_cert;
  std::string alpha_key;
  std::string beta_cert;
  std::string beta_key;
  std::string other_ca;
  std::string other_cert;
  std::string other_key;

  ~TestCerts() {
    for (const std::string *path : {&ca, &alpha_cert, &alpha_key, &beta_cert,
                                    &beta_key, &other_ca, &other_cert,
                                    &other_key}) {
      if (!path->empty()) {
        unlink(path->c_str());
      }
    }
    if (!directory.empty()) {
      rmdir(directory.c_str());
    }
  }
};

inline absl::StatusOr<CertPtr> MakeCertificate(EVP_PKEY *subject_key,
                                               EVP_PKEY *issuer_key,
                                               X509 *issuer, const char *cn,
                                               int serial, bool is_ca) {
  CertPtr cert(X509_new());
  if (cert == nullptr) {
    return absl::InternalError("X509_new failed");
  }
  if (X509_set_version(cert.get(), 2) != 1 ||
      ASN1_INTEGER_set(X509_get_serialNumber(cert.get()), serial) != 1 ||
      !SetLifetime(cert.get()) ||
      X509_set_pubkey(cert.get(), subject_key) != 1) {
    return absl::InternalError("failed to fill a test certificate");
  }
  X509_NAME *subject = X509_get_subject_name(cert.get());
  if (X509_NAME_add_entry_by_txt(
          subject, "CN", MBSTRING_ASC,
          reinterpret_cast<const unsigned char *>(cn), -1, -1, 0) != 1) {
    return absl::InternalError("failed to set a test certificate name");
  }
  X509_NAME *issuer_name =
      issuer == nullptr ? subject : X509_get_subject_name(issuer);
  if (X509_set_issuer_name(cert.get(), issuer_name) != 1) {
    return absl::InternalError("failed to set a test certificate issuer");
  }
  if (!AddBasicConstraints(cert.get(), is_ca)) {
    return absl::InternalError("failed to set basic constraints");
  }
  if (X509_sign(cert.get(), issuer_key, EVP_sha256()) == 0) {
    return absl::InternalError("failed to sign a test certificate");
  }
  return cert;
}

inline absl::StatusOr<std::unique_ptr<TestCerts>> MintTestCerts() {
  auto certs = std::make_unique<TestCerts>();
  std::string dir = "/tmp/subspace_mtlsXXXXXX";
  if (mkdtemp(dir.data()) == nullptr) {
    return absl::InternalError("mkdtemp failed");
  }
  certs->directory = dir;

  absl::StatusOr<KeyPtr> ca_key = GenerateKey();
  if (!ca_key.ok()) {
    return ca_key.status();
  }
  absl::StatusOr<CertPtr> ca =
      MakeCertificate(ca_key->get(), ca_key->get(), nullptr, "subspace-test-ca",
                      1, true);
  if (!ca.ok()) {
    return ca.status();
  }
  absl::StatusOr<KeyPtr> alpha_key = GenerateKey();
  absl::StatusOr<KeyPtr> beta_key = GenerateKey();
  if (!alpha_key.ok()) {
    return alpha_key.status();
  }
  if (!beta_key.ok()) {
    return beta_key.status();
  }
  absl::StatusOr<CertPtr> alpha = MakeCertificate(
      alpha_key->get(), ca_key->get(), ca->get(), "alpha", 2, false);
  absl::StatusOr<CertPtr> beta = MakeCertificate(
      beta_key->get(), ca_key->get(), ca->get(), "beta", 3, false);
  if (!alpha.ok()) {
    return alpha.status();
  }
  if (!beta.ok()) {
    return beta.status();
  }

  absl::StatusOr<KeyPtr> other_ca_key = GenerateKey();
  if (!other_ca_key.ok()) {
    return other_ca_key.status();
  }
  absl::StatusOr<CertPtr> other_ca =
      MakeCertificate(other_ca_key->get(), other_ca_key->get(), nullptr,
                      "other-ca", 1, true);
  if (!other_ca.ok()) {
    return other_ca.status();
  }
  absl::StatusOr<KeyPtr> other_key = GenerateKey();
  if (!other_key.ok()) {
    return other_key.status();
  }
  absl::StatusOr<CertPtr> other = MakeCertificate(
      other_key->get(), other_ca_key->get(), other_ca->get(), "other", 2,
      false);
  if (!other.ok()) {
    return other.status();
  }

  certs->ca = dir + "/ca.pem";
  certs->alpha_cert = dir + "/alpha.pem";
  certs->alpha_key = dir + "/alpha.key";
  certs->beta_cert = dir + "/beta.pem";
  certs->beta_key = dir + "/beta.key";
  certs->other_ca = dir + "/other-ca.pem";
  certs->other_cert = dir + "/other.pem";
  certs->other_key = dir + "/other.key";
  if (absl::Status s = WriteCert(certs->ca, ca->get()); !s.ok()) {
    return s;
  }
  if (absl::Status s = WriteCert(certs->alpha_cert, alpha->get()); !s.ok()) {
    return s;
  }
  if (absl::Status s = WriteKey(certs->alpha_key, alpha_key->get()); !s.ok()) {
    return s;
  }
  if (absl::Status s = WriteCert(certs->beta_cert, beta->get()); !s.ok()) {
    return s;
  }
  if (absl::Status s = WriteKey(certs->beta_key, beta_key->get()); !s.ok()) {
    return s;
  }
  if (absl::Status s = WriteCert(certs->other_ca, other_ca->get()); !s.ok()) {
    return s;
  }
  if (absl::Status s = WriteCert(certs->other_cert, other->get()); !s.ok()) {
    return s;
  }
  if (absl::Status s = WriteKey(certs->other_key, other_key->get()); !s.ok()) {
    return s;
  }
  return certs;
}

} // namespace
} // namespace subspace::async

#endif // _COMMON_ASYNC_MTLS_TEST_CERTS_H
