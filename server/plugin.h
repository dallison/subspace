
#ifndef _xSERVERPLUGIN_H
#define _xSERVERPLUGIN_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "toolbelt/logging.h"
#include <dlfcn.h>
#include <string>

namespace subspace {
class Server;

struct PluginContext  {
  PluginContext(const std::string &name) : logger(name) {}
  virtual ~PluginContext() = default;
  toolbelt::Logger logger;
};

// Plugins allow an externally loaded module to handle occurences in the
// server.  It is envisioned that they can be used for adding additional
// channels, etc.  The server runs in a single thread so plugins must
// behave themselves and not block for long periods of time.  Access to
// the current coroutine is in `co::self`.
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

private:
  PluginInterfaceFunctions functions_;
  std::unique_ptr<PluginContext> ctx_;
};
} // namespace subspace
#endif // _xSERVERPLUGIN_H