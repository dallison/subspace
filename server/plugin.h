
#ifndef _xSERVERPLUGIN_H
#define _xSERVERPLUGIN_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "common/client_buffer.h"
#include "toolbelt/logging.h"
#include <cstddef>
#include <dlfcn.h>
#include <memory>
#include <string>
#include <sys/types.h>

namespace co {
class CoroutineScheduler;
}

namespace subspace {
class Server;

struct PluginContext  {
  PluginContext(const std::string &name) : logger(name) {}
  virtual ~PluginContext() = default;
  toolbelt::Logger logger;
  co::CoroutineScheduler *scheduler = nullptr;
};

// Payload passed to an egress plugin. `bytes` does not include the prefix.
// The memory belongs to the published slot and may be read-only, so a plugin
// that changes the message returns a new span kept alive by `keepalive`.
struct PluginPayload {
  absl::Span<const std::byte> bytes;
  std::shared_ptr<void> keepalive;
  bool modified = false;
};

// Result of an ingress plugin. See PluginInterfaceFunctions::onReceive.
struct PluginReceiveResult {
  ssize_t length = -1;
  bool modified = false;
};

// Plugins allow an externally loaded module to handle occurences in the
// server.  It is envisioned that they can be used for adding additional
// channels, etc.  The server runs in a single thread so plugins must
// behave themselves and not block for long periods of time.  Access to
// the current coroutine is in `co::self`.  onSend and onReceive can run
// at the same time as each other when the server is using more than one
// thread.
struct PluginInterfaceFunctions {
  absl::Status (*onStartup)(Server &s, const std::string& name, PluginContext *ctx);
  void (*onReady)(Server &s, PluginContext *ctx);
  void (*onShutdown)(PluginContext *ctx);

  void (*onNewChannel)(Server &s, const std::string &channel_name, PluginContext *ctx);
  void (*onRemoveChannel)(Server &s, const std::string &channel_name, PluginContext *ctx);
  void (*onNewPublisher)(Server &s, const std::string &channel_name, int publisher_id, PluginContext *ctx);
  void (*onRemovePublisher)(Server &s, const std::string &channel_name, int publisher_id, PluginContext *ctx);
  void (*onNewSubscriber)(Server &s, const std::string &channel_name, int subscriber_id, PluginContext *ctx);
  void (*onRemoveSubscriber)(Server &s, const std::string &channel_name, int subscriber_id, PluginContext *ctx);
  // Gives plugins a chance to release client-allocated memory by allocator
  // handle. Return true if the plugin recognized and freed the allocation,
  // false to let another plugin or the default cleanup handle it.
  absl::StatusOr<bool> (*onFreeClientBuffer)(
      Server &s, const ClientBufferHandleMetadata &metadata,
      PluginContext *ctx);

  // Called for a message about to leave over a native bridge or the tunnel.
  // `payload` is the message bytes without the prefix. Return that payload
  // to leave it unchanged. To replace it, return a new span and keep it
  // alive with `keepalive`, and set `modified`. A returned error skips this
  // plugin and the previous payload is kept. A null function means the
  // plugin does not see egress traffic.
  absl::StatusOr<PluginPayload> (*onSend)(Server &s,
                                          const std::string &channel_name,
                                          PluginPayload payload,
                                          PluginContext *ctx);

  // Called for a message that has arrived over a native bridge or the tunnel
  // and is about to be published. `data` is the payload without the prefix
  // and can be changed in place, up to `max_length` bytes. `length` follows
  // the cruise tunnel plugin: a positive value is the payload length to
  // publish, 0 drops the message and skips later plugins, and -1 leaves the
  // payload unchanged by this plugin. Set `modified` when the bytes changed
  // even if the length did not, so the checksum can be recomputed. A null
  // function means the plugin does not see ingress traffic.
  PluginReceiveResult (*onReceive)(Server &s, const std::string &channel_name,
                                   std::byte *data, size_t length,
                                   size_t max_length, int32_t slot_id,
                                   PluginContext *ctx);
};

class PluginInterface {
public:
  PluginInterface(const PluginInterfaceFunctions &functions, std::unique_ptr<PluginContext> ctx = nullptr)
      : functions_(functions), ctx_(std::move(ctx)) {}

  absl::Status OnStartup(Server &s, const std::string& name) {
    return functions_.onStartup(s, name, ctx_.get());
  }

  void OnReady(Server &s) { functions_.onReady(s, ctx_.get()); }

  void OnShutdown() { functions_.onShutdown(ctx_.get()); }

  void OnNewChannel(Server &s, const std::string &channel_name) {
    functions_.onNewChannel(s, channel_name, ctx_.get());
  }

  void OnRemoveChannel(Server &s, const std::string &channel_name) {
    functions_.onRemoveChannel(s, channel_name, ctx_.get());
  }

  void OnNewPublisher(Server &s, const std::string &channel_name,
                      int publisher_id) {
    functions_.onNewPublisher(s, channel_name, publisher_id, ctx_.get());
  }

  void OnRemovePublisher(Server &s, const std::string &channel_name,
                         int publisher_id) {
    functions_.onRemovePublisher(s, channel_name, publisher_id, ctx_.get());
  }

  void OnNewSubscriber(Server &s, const std::string &channel_name,
                       int subscriber_id) {
    functions_.onNewSubscriber(s, channel_name, subscriber_id, ctx_.get());
  }

  void OnRemoveSubscriber(Server &s, const std::string &channel_name,
                          int subscriber_id) {
    functions_.onRemoveSubscriber(s, channel_name, subscriber_id, ctx_.get());
  }

  absl::StatusOr<bool> OnFreeClientBuffer(
      Server &s, const ClientBufferHandleMetadata &metadata) {
    if (functions_.onFreeClientBuffer == nullptr) {
      return false;
    }
    return functions_.onFreeClientBuffer(s, metadata, ctx_.get());
  }

  bool HasOnSend() const { return functions_.onSend != nullptr; }

  absl::StatusOr<PluginPayload> OnSend(Server &s,
                                       const std::string &channel_name,
                                       PluginPayload payload) {
    return functions_.onSend(s, channel_name, std::move(payload), ctx_.get());
  }

  bool HasOnReceive() const { return functions_.onReceive != nullptr; }

  PluginReceiveResult OnReceive(Server &s, const std::string &channel_name,
                                std::byte *data, size_t length,
                                size_t max_length, int32_t slot_id) {
    return functions_.onReceive(s, channel_name, data, length, max_length,
                                slot_id, ctx_.get());
  }

  void SetScheduler(co::CoroutineScheduler &scheduler) {
    if (ctx_) {
      ctx_->scheduler = &scheduler;
    }
  }

private:
  PluginInterfaceFunctions functions_;
  std::unique_ptr<PluginContext> ctx_;
};
} // namespace subspace
#endif // _xSERVERPLUGIN_H