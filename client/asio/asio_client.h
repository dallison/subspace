// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

#ifndef CLIENT_ASIO_ASIO_CLIENT_H_
#define CLIENT_ASIO_ASIO_CLIENT_H_

#include "client/client.h"

#include "absl/status/statusor.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/thread_pool.hpp>

#include <memory>
#include <string>

namespace subspace {

class AsioPublisher;
class AsioSubscriber;

class AsioClient : public std::enable_shared_from_this<AsioClient> {
public:
  ~AsioClient();

  AsioClient(const AsioClient &) = delete;
  AsioClient &operator=(const AsioClient &) = delete;

  template <typename CompletionToken>
  static auto AsyncCreate(boost::asio::io_context &io,
                          std::string server_socket, std::string client_name,
                          CompletionToken &&token);

  boost::asio::io_context &IoContext() const { return *io_; }

  void SetDebug(bool enabled);

  template <typename CompletionToken>
  auto AsyncCreatePublisher(std::string channel_name, PublisherOptions options,
                            CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncCreateSubscriber(std::string channel_name,
                             SubscriberOptions options,
                             CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncGetChannelInfo(std::string channel_name, CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncGetChannelInfo(CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncGetChannelStats(std::string channel_name, CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncGetChannelStats(CompletionToken &&token);

  template <typename CompletionToken>
  auto AsyncChannelExists(std::string channel_name, CompletionToken &&token);

private:
  friend class AsioPublisher;
  friend class AsioSubscriber;

  explicit AsioClient(boost::asio::io_context &io);

  void ReleasePublisher(Publisher publisher);
  void ReleaseSubscriber(Subscriber subscriber);

  boost::asio::io_context *io_;
  boost::asio::thread_pool control_pool_{1};
  std::shared_ptr<Client> client_;
};

} // namespace subspace

#include "client/asio/asio_client.inl"
#include "client/asio/asio_publisher.h"
#include "client/asio/asio_subscriber.h"

#endif // CLIENT_ASIO_ASIO_CLIENT_H_
