// Copyright 2023-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#include <jni.h>
#include <string>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "client/client.h"

#define JNI_METHOD(return_type, class_name, method_name) \
  JNIEXPORT return_type JNICALL                          \
      Java_com_subspace_##class_name##_##method_name

namespace {

struct NativeClient {
  std::shared_ptr<subspace::Client> client;
};

struct NativePublisher {
  std::shared_ptr<subspace::Client> client;
  std::unique_ptr<subspace::Publisher> publisher;
};

struct NativeSubscriber {
  std::shared_ptr<subspace::Client> client;
  std::unique_ptr<subspace::Subscriber> subscriber;
};

void ThrowException(JNIEnv *env, const char *class_name, const char *msg) {
  jclass cls = env->FindClass(class_name);
  if (cls != nullptr) {
    env->ThrowNew(cls, msg);
    env->DeleteLocalRef(cls);
  }
}

void ThrowSubspaceException(JNIEnv *env, const std::string &msg) {
  ThrowException(env, "com/subspace/SubspaceException", msg.c_str());
}

} // namespace

extern "C" {

// ---------------------------------------------------------------------------
// SubspaceClient
// ---------------------------------------------------------------------------

JNI_METHOD(jlong, SubspaceClient, nativeCreate)(JNIEnv *env, jobject,
                                                jstring socket_path,
                                                jstring client_name) {
  const char *socket_cstr = env->GetStringUTFChars(socket_path, nullptr);
  const char *name_cstr = env->GetStringUTFChars(client_name, nullptr);

  auto result = subspace::Client::Create(socket_cstr, name_cstr);
  env->ReleaseStringUTFChars(socket_path, socket_cstr);
  env->ReleaseStringUTFChars(client_name, name_cstr);

  if (!result.ok()) {
    ThrowSubspaceException(env, result.status().ToString());
    return 0;
  }

  auto *nc = new NativeClient{std::move(*result)};
  return reinterpret_cast<jlong>(nc);
}

JNI_METHOD(void, SubspaceClient, nativeDestroy)(JNIEnv *, jobject,
                                                jlong handle) {
  delete reinterpret_cast<NativeClient *>(handle);
}

JNI_METHOD(jlong, SubspaceClient, nativeCreatePublisher)(
    JNIEnv *env, jobject, jlong handle, jstring channel_name, jint slot_size,
    jint num_slots, jboolean reliable) {
  auto *nc = reinterpret_cast<NativeClient *>(handle);
  const char *name_cstr = env->GetStringUTFChars(channel_name, nullptr);

  subspace::PublisherOptions opts;
  opts.SetReliable(reliable);

  auto result =
      nc->client->CreatePublisher(name_cstr, slot_size, num_slots, opts);
  env->ReleaseStringUTFChars(channel_name, name_cstr);

  if (!result.ok()) {
    ThrowSubspaceException(env, result.status().ToString());
    return 0;
  }

  auto *np = new NativePublisher{nc->client,
                                 std::make_unique<subspace::Publisher>(
                                     std::move(*result))};
  return reinterpret_cast<jlong>(np);
}

JNI_METHOD(jlong, SubspaceClient, nativeCreateSubscriber)(
    JNIEnv *env, jobject, jlong handle, jstring channel_name) {
  auto *nc = reinterpret_cast<NativeClient *>(handle);
  const char *name_cstr = env->GetStringUTFChars(channel_name, nullptr);

  auto result = nc->client->CreateSubscriber(name_cstr);
  env->ReleaseStringUTFChars(channel_name, name_cstr);

  if (!result.ok()) {
    ThrowSubspaceException(env, result.status().ToString());
    return 0;
  }

  auto *ns = new NativeSubscriber{nc->client,
                                  std::make_unique<subspace::Subscriber>(
                                      std::move(*result))};
  return reinterpret_cast<jlong>(ns);
}

// ---------------------------------------------------------------------------
// SubspacePublisher
// ---------------------------------------------------------------------------

JNI_METHOD(jobject, SubspacePublisher, nativeGetMessageBuffer)(
    JNIEnv *env, jobject, jlong handle, jint max_size) {
  auto *np = reinterpret_cast<NativePublisher *>(handle);
  auto result = np->publisher->GetMessageBuffer(max_size);
  if (!result.ok()) {
    ThrowSubspaceException(env, result.status().ToString());
    return nullptr;
  }
  void *buf = *result;
  if (buf == nullptr) {
    return nullptr;
  }
  int32_t slot_size = np->publisher->SlotSize();
  return env->NewDirectByteBuffer(buf, max_size > 0 ? max_size : slot_size);
}

JNI_METHOD(jlong, SubspacePublisher, nativePublishMessage)(JNIEnv *env,
                                                           jobject,
                                                           jlong handle,
                                                           jlong message_size) {
  auto *np = reinterpret_cast<NativePublisher *>(handle);
  auto result = np->publisher->PublishMessage(message_size);
  if (!result.ok()) {
    ThrowSubspaceException(env, result.status().ToString());
    return -1;
  }
  return result->ordinal;
}

JNI_METHOD(void, SubspacePublisher, nativeCancelPublish)(JNIEnv *, jobject,
                                                         jlong handle) {
  auto *np = reinterpret_cast<NativePublisher *>(handle);
  np->publisher->CancelPublish();
}

JNI_METHOD(jint, SubspacePublisher, nativeGetPollFd)(JNIEnv *, jobject,
                                                     jlong handle) {
  auto *np = reinterpret_cast<NativePublisher *>(handle);
  struct pollfd pfd = np->publisher->GetPollFd();
  return pfd.fd;
}

JNI_METHOD(jint, SubspacePublisher, nativeGetSlotSize)(JNIEnv *, jobject,
                                                       jlong handle) {
  auto *np = reinterpret_cast<NativePublisher *>(handle);
  return np->publisher->SlotSize();
}

JNI_METHOD(jstring, SubspacePublisher, nativeGetName)(JNIEnv *env, jobject,
                                                      jlong handle) {
  auto *np = reinterpret_cast<NativePublisher *>(handle);
  return env->NewStringUTF(np->publisher->Name().c_str());
}

JNI_METHOD(void, SubspacePublisher, nativeDestroy)(JNIEnv *, jobject,
                                                   jlong handle) {
  delete reinterpret_cast<NativePublisher *>(handle);
}

// ---------------------------------------------------------------------------
// SubspaceSubscriber
// ---------------------------------------------------------------------------

JNI_METHOD(jobject, SubspaceSubscriber, nativeReadMessage)(JNIEnv *env,
                                                           jobject,
                                                           jlong handle,
                                                           jboolean newest) {
  auto *ns = reinterpret_cast<NativeSubscriber *>(handle);
  auto mode = newest ? subspace::ReadMode::kReadNewest
                     : subspace::ReadMode::kReadNext;
  auto result = ns->subscriber->ReadMessage(mode);
  if (!result.ok()) {
    ThrowSubspaceException(env, result.status().ToString());
    return nullptr;
  }
  const subspace::Message &msg = *result;
  if (msg.length == 0) {
    return nullptr;
  }

  // Build a SubspaceMessage object to return.
  jclass msg_class = env->FindClass("com/subspace/SubspaceMessage");
  if (msg_class == nullptr) {
    return nullptr;
  }
  jmethodID ctor = env->GetMethodID(msg_class, "<init>",
                                    "(Ljava/nio/ByteBuffer;JJII)V");
  if (ctor == nullptr) {
    return nullptr;
  }

  jobject byte_buffer = env->NewDirectByteBuffer(
      const_cast<void *>(msg.buffer), msg.length);
  jobject message = env->NewObject(msg_class, ctor, byte_buffer,
                                   static_cast<jlong>(msg.timestamp),
                                   static_cast<jlong>(msg.ordinal),
                                   static_cast<jint>(msg.slot_id),
                                   static_cast<jint>(msg.length));
  return message;
}

JNI_METHOD(jint, SubspaceSubscriber, nativeGetPollFd)(JNIEnv *, jobject,
                                                      jlong handle) {
  auto *ns = reinterpret_cast<NativeSubscriber *>(handle);
  struct pollfd pfd = ns->subscriber->GetPollFd();
  return pfd.fd;
}

JNI_METHOD(jstring, SubspaceSubscriber, nativeGetName)(JNIEnv *env, jobject,
                                                       jlong handle) {
  auto *ns = reinterpret_cast<NativeSubscriber *>(handle);
  return env->NewStringUTF(ns->subscriber->Name().c_str());
}

JNI_METHOD(void, SubspaceSubscriber, nativeDestroy)(JNIEnv *, jobject,
                                                    jlong handle) {
  delete reinterpret_cast<NativeSubscriber *>(handle);
}

} // extern "C"
