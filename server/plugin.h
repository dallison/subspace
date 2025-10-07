
#ifndef _xSERVERPLUGIN_H
#define _xSERVERPLUGIN_H

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include <dlfcn.h>
#include <string>

namespace subspace {
class Server;

// Plugins allow an externally loaded module to handle occurences in the
// server.  It is envisioned that they can be used for adding additional
// channels, etc.  The server runs in a single thread so plugins must
// behave themselves and not block for long periods of time.  Access to
// the current coroutine is in `co::self`.
struct PluginInterfaceFunctions {
  absl::Status (*onStartup)(Server &s, const std::string& name);
  void (*onShutdown)();

  void (*onNewChannel)(Server &s, const std::string &channel_name);
  void (*onRemoveChannel)(Server &s, const std::string &channel_name);
  void (*onNewPublisher)(Server &s, const std::string &channel_name,
                         int publisher_id);
  void (*onRemovePublisher)(Server &s, const std::string &channel_name,
                            int publisher_id);
  void (*onNewSubscriber)(Server &s, const std::string &channel_name,
                          int subscriber_id);
  void (*onRemoveSubscriber)(Server &s, const std::string &channel_name,
                             int subscriber_id);
};

class PluginInterface {
public:
  PluginInterface(const PluginInterfaceFunctions &functions)
      : functions_(functions) {}

  absl::Status OnStartup(Server &s, const std::string& name) {
    return functions_.onStartup(s, name);
  }

  void OnShutdown() { functions_.onShutdown(); }

  void OnNewChannel(Server &s, const std::string &channel_name) {
    functions_.onNewChannel(s, channel_name);
  }

  void OnRemoveChannel(Server &s, const std::string &channel_name) {
    functions_.onRemoveChannel(s, channel_name);
  }

  void OnNewPublisher(Server &s, const std::string &channel_name,
                      int publisher_id) {
    functions_.onNewPublisher(s, channel_name, publisher_id);
  }

  void OnRemovePublisher(Server &s, const std::string &channel_name,
                         int publisher_id) {
    functions_.onRemovePublisher(s, channel_name, publisher_id);
  }

  void OnNewSubscriber(Server &s, const std::string &channel_name,
                       int subscriber_id) {
    functions_.onNewSubscriber(s, channel_name, subscriber_id);
  }

  void OnRemoveSubscriber(Server &s, const std::string &channel_name,
                          int subscriber_id) {
    functions_.onRemoveSubscriber(s, channel_name, subscriber_id);
  }

private:
  PluginInterfaceFunctions functions_;
};
} // namespace subspace
#endif // _xSERVERPLUGIN_H