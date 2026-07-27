#include "dashboard/engine.h"

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "toolbelt/clock.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

namespace subspace::dashboard {
namespace {

constexpr int kPayloadSize = sizeof(uint64_t);
constexpr uint64_t kShortWaitNs = 100'000;

size_t PublishBatchSize(int32_t slots) {
  return static_cast<size_t>(std::max(1, slots - 1));
}

} // namespace

DashboardEngine::DashboardEngine(Config config) : config_(std::move(config)) {
  const int32_t initial_slots = slots_.Current();
  const std::string initial_channel =
      ChannelNameForSlots(config_.channel, initial_slots);
  published_channel_ = {
      .name = initial_channel,
      .slots = initial_slots,
      .generation = slots_.Generation(),
  };
  active_channel_ = published_channel_;
  metrics_.SetTargetRate(rate_.Current().label);
  metrics_.ResetForChannel(initial_channel, initial_slots);
}

DashboardEngine::~DashboardEngine() {
  RequestStop();
  JoinWorkers();
  ClosePipe(publisher_control_);
  ClosePipe(subscriber_stop_);
  ClosePipe(sampler_stop_);
}

absl::Status DashboardEngine::Init() {
  if (auto status = CreateNonBlockingPipe(publisher_control_); !status.ok()) {
    return status;
  }
  if (auto status = CreateNonBlockingPipe(subscriber_stop_); !status.ok()) {
    return status;
  }
  if (auto status = CreateNonBlockingPipe(sampler_stop_); !status.ok()) {
    return status;
  }

  auto publisher_client =
      subspace::Client::Create(config_.socket, "subspace-dashboard-publisher");
  if (!publisher_client.ok()) {
    return publisher_client.status();
  }
  publisher_client_ = std::move(*publisher_client);

  auto publisher = CreatePublisher(published_channel_);
  if (!publisher.ok()) {
    return publisher.status();
  }
  publisher_.emplace(std::move(*publisher));

  auto subscriber_client =
      subspace::Client::Create(config_.socket, "subspace-dashboard-subscriber");
  if (!subscriber_client.ok()) {
    return subscriber_client.status();
  }
  subscriber_client_ = std::move(*subscriber_client);

  auto subscriber = CreateSubscriber(active_channel_);
  if (!subscriber.ok()) {
    return subscriber.status();
  }
  subscriber_.emplace(std::move(*subscriber));
  return absl::OkStatus();
}

void DashboardEngine::StartTasks(retro::Cpp20Application &app) {
  if (tasks_started_) {
    return;
  }
  tasks_started_ = true;
  app.Spawn([this](co20::Coroutine &c) -> co20::Task { return SamplerTask(c); },
            "dashboard-sampler", sampler_stop_[0]);
  subscriber_thread_ = std::thread([this] { SubscriberLoop(); });
  publisher_thread_ = std::thread([this] { PublisherLoop(); });
}

void DashboardEngine::RequestStop() {
  if (stopping_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  rate_changed_.notify_all();
  Wake(publisher_control_[1]);
  Wake(subscriber_stop_[1]);
  Wake(sampler_stop_[1]);
}

bool DashboardEngine::IncreaseRate() {
  const char *label;
  {
    std::lock_guard lock(rate_mutex_);
    if (!rate_.Increase()) {
      return false;
    }
    label = rate_.Current().label;
  }
  {
    std::lock_guard lock(metrics_mutex_);
    metrics_.SetTargetRate(label);
  }
  NotifyRateChanged();
  return true;
}

bool DashboardEngine::DecreaseRate() {
  const char *label;
  {
    std::lock_guard lock(rate_mutex_);
    if (!rate_.Decrease()) {
      return false;
    }
    label = rate_.Current().label;
  }
  {
    std::lock_guard lock(metrics_mutex_);
    metrics_.SetTargetRate(label);
  }
  NotifyRateChanged();
  return true;
}

bool DashboardEngine::IncreaseSlots() {
  {
    std::lock_guard lock(rate_mutex_);
    if (!slots_.Increase()) {
      return false;
    }
  }
  {
    std::lock_guard lock(metrics_mutex_);
    metrics_.ResetStatistics();
    dropped_since_warning_ = 0;
  }
  NotifyChannelChanged();
  return true;
}

bool DashboardEngine::DecreaseSlots() {
  {
    std::lock_guard lock(rate_mutex_);
    if (!slots_.Decrease()) {
      return false;
    }
  }
  {
    std::lock_guard lock(metrics_mutex_);
    metrics_.ResetStatistics();
    dropped_since_warning_ = 0;
  }
  NotifyChannelChanged();
  return true;
}

void DashboardEngine::RecordWarning(std::string warning) {
  std::lock_guard lock(metrics_mutex_);
  metrics_.RecordWarning(std::move(warning));
}

absl::StatusOr<subspace::Publisher>
DashboardEngine::CreatePublisher(const ChannelProfile &profile) {
  return publisher_client_->CreatePublisher(
      profile.name, kPayloadSize, profile.slots,
      subspace::PublisherOptions().SetFixedSize(true).SetType(
          "subspace.dashboard.Timestamp"));
}

absl::StatusOr<subspace::Subscriber>
DashboardEngine::CreateSubscriber(const ChannelProfile &profile) {
  subspace::SubscriberOptions options;
  options.SetType("subspace.dashboard.Timestamp");
  options.SetLogDroppedMessages(false);
  auto subscriber =
      subscriber_client_->CreateSubscriber(profile.name, options);
  if (!subscriber.ok()) {
    return subscriber.status();
  }
  auto callback_status = subscriber->RegisterDroppedMessageCallback(
      [this](subspace::Subscriber *, int64_t count) {
        if (count > 0) {
          std::lock_guard lock(metrics_mutex_);
          metrics_.RecordDropped(static_cast<uint64_t>(count));
          dropped_since_warning_ += static_cast<uint64_t>(count);
        }
      });
  if (!callback_status.ok()) {
    return callback_status;
  }
  return std::move(*subscriber);
}

void DashboardEngine::PublisherLoop() {
  uint64_t deadline_ns = toolbelt::Now();
  uint64_t generation;
  uint64_t channel_generation;
  size_t publish_batch_size;
  {
    std::lock_guard lock(rate_mutex_);
    generation = rate_.Generation();
    channel_generation = published_channel_.generation;
    publish_batch_size = PublishBatchSize(published_channel_.slots);
  }
  uint64_t published_since_record = 0;

  auto record_published = [&] {
    if (published_since_record != 0) {
      RecordPublished(published_since_record);
      published_since_record = 0;
    }
  };

  while (!IsStopping()) {
    RateController::Setting setting;
    uint64_t current_generation;
    ChannelProfile requested_channel;
    {
      std::lock_guard lock(rate_mutex_);
      setting = rate_.Current();
      current_generation = rate_.Generation();
      requested_channel = {
          .name = ChannelNameForSlots(config_.channel, slots_.Current()),
          .slots = slots_.Current(),
          .generation = slots_.Generation(),
      };
    }
    if (channel_generation != requested_channel.generation) {
      record_published();
      auto publisher = CreatePublisher(requested_channel);
      if (!publisher.ok()) {
        std::lock_guard lock(rate_mutex_);
        if (slots_.Generation() != requested_channel.generation) {
          continue;
        }
        ReportError(publisher.status());
        return;
      }
      publisher_.emplace(std::move(*publisher));
      publish_batch_size = PublishBatchSize(requested_channel.slots);
      channel_generation = requested_channel.generation;
      {
        std::lock_guard lock(rate_mutex_);
        published_channel_ = requested_channel;
      }
      RecordWarning(absl::StrFormat("Publisher switched to %s (%d slots)",
                                    requested_channel.name,
                                    requested_channel.slots));
      Wake(subscriber_stop_[1]);
      continue;
    }
    if (generation != current_generation) {
      record_published();
      {
        std::lock_guard lock(metrics_mutex_);
        metrics_.ResetStatistics();
        dropped_since_warning_ = 0;
      }
      generation = current_generation;
      deadline_ns = toolbelt::Now();
    }

    if (setting.hz == 0.0) {
      record_published();
      std::unique_lock lock(rate_mutex_);
      rate_changed_.wait(lock, [&] {
        return IsStopping() || rate_.Generation() != generation ||
               slots_.Generation() != channel_generation;
      });
      continue;
    }

    uint64_t now_ns = toolbelt::Now();
    if (now_ns < deadline_ns) {
      record_published();
      const uint64_t remaining_ns = deadline_ns - now_ns;
      if (remaining_ns > kShortWaitNs) {
        std::unique_lock lock(rate_mutex_);
        rate_changed_.wait_for(
            lock, std::chrono::nanoseconds(remaining_ns), [&] {
              return IsStopping() || rate_.Generation() != generation ||
                     slots_.Generation() != channel_generation;
            });
      } else {
        std::this_thread::yield();
      }
      continue;
    }

    auto buffer = publisher_->GetMessageBuffer(kPayloadSize);
    if (!buffer.ok()) {
      record_published();
      ReportError(buffer.status());
      return;
    }
    if (*buffer == nullptr) {
      record_published();
      struct pollfd fds[2] = {
          publisher_->GetPollFd(),
          {.fd = publisher_control_[0], .events = POLLIN, .revents = 0},
      };
      int result;
      do {
        result = poll(fds, 2, -1);
      } while (result < 0 && errno == EINTR);
      if (result < 0) {
        ReportError(absl::ErrnoToStatus(errno, "publisher poll failed"));
        return;
      }
      if ((fds[1].revents & POLLIN) != 0) {
        Drain(publisher_control_[0]);
      }
      continue;
    }

    const uint64_t publish_time_ns = toolbelt::Now();
    std::memcpy(*buffer, &publish_time_ns, sizeof(publish_time_ns));
    auto published = publisher_->PublishMessage(kPayloadSize);
    if (!published.ok()) {
      record_published();
      ReportError(published.status());
      return;
    }
    ++published_since_record;
    if (published_since_record >= publish_batch_size) {
      record_published();
    }

    const uint64_t period_ns = std::max<uint64_t>(
        1, static_cast<uint64_t>(1'000'000'000.0 / setting.hz));
    deadline_ns += period_ns;
    now_ns = toolbelt::Now();
    if (deadline_ns + period_ns * 4 < now_ns) {
      deadline_ns = now_ns + period_ns;
    }
  }
  record_published();
}

void DashboardEngine::SubscriberLoop() {
  uint64_t channel_generation;
  int32_t channel_slots;
  std::string channel_name;
  {
    std::lock_guard lock(rate_mutex_);
    channel_generation = active_channel_.generation;
    channel_slots = active_channel_.slots;
    channel_name = active_channel_.name;
  }
  struct pollfd fds[2] = {
      subscriber_->GetPollFd(),
      {.fd = subscriber_stop_[0], .events = POLLIN, .revents = 0},
  };
  std::vector<uint64_t> latencies;
  latencies.reserve(PublishBatchSize(channel_slots));

  while (!IsStopping()) {
    int result;
    do {
      result = poll(fds, 2, -1);
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
      ReportError(absl::ErrnoToStatus(errno, "subscriber poll failed"));
      return;
    }
    if ((fds[1].revents & POLLIN) != 0) {
      Drain(subscriber_stop_[0]);
      if (IsStopping()) {
        break;
      }
      ChannelProfile published_channel;
      {
        std::lock_guard lock(rate_mutex_);
        published_channel = published_channel_;
      }
      if (published_channel.generation != channel_generation) {
        subscriber_.reset();
        for (;;) {
          if (IsStopping()) {
            return;
          }
          auto subscriber = CreateSubscriber(published_channel);
          ChannelProfile latest_channel;
          {
            std::lock_guard lock(rate_mutex_);
            latest_channel = published_channel_;
          }
          if (!subscriber.ok()) {
            if (latest_channel.generation != published_channel.generation) {
              published_channel = std::move(latest_channel);
              continue;
            }
            ReportError(subscriber.status());
            return;
          }
          if (latest_channel.generation != published_channel.generation) {
            published_channel = std::move(latest_channel);
            continue;
          }
          subscriber_.emplace(std::move(*subscriber));
          break;
        }
        channel_generation = published_channel.generation;
        channel_name = published_channel.name;
        fds[0] = subscriber_->GetPollFd();
        latencies.clear();
        latencies.reserve(PublishBatchSize(published_channel.slots));
        {
          std::lock_guard lock(rate_mutex_);
          active_channel_ = published_channel;
        }
        {
          std::lock_guard lock(metrics_mutex_);
          dropped_since_warning_ = 0;
          metrics_.ResetForChannel(published_channel.name,
                                   published_channel.slots);
        }
        RecordWarning(absl::StrFormat("Subscriber switched to %s (%d slots)",
                                      published_channel.name,
                                      published_channel.slots));
        continue;
      }
    }
    if ((fds[0].revents & (POLLIN | POLLERR | POLLHUP)) == 0) {
      continue;
    }

    latencies.clear();
    bool first_message = true;
    for (;;) {
      auto message = first_message
                         ? subscriber_->ReadMessage()
                         : subscriber_->ReadMessageFromBatch();
      first_message = false;
      if (!message.ok()) {
        RecordReceivedBatch(latencies);
        ReportError(message.status());
        return;
      }
      if (message->length == 0) {
        break;
      }
      if (message->length != kPayloadSize || message->buffer == nullptr) {
        RecordReceivedBatch(latencies);
        ReportError(absl::DataLossError(absl::StrFormat(
            "channel %s received a %d-byte payload; expected %d",
            channel_name, message->length, kPayloadSize)));
        return;
      }

      uint64_t publish_time_ns = 0;
      std::memcpy(&publish_time_ns, message->buffer, sizeof(publish_time_ns));
      const uint64_t receive_time_ns = toolbelt::Now();
      latencies.push_back(receive_time_ns >= publish_time_ns
                              ? receive_time_ns - publish_time_ns
                              : 0);
    }
    RecordReceivedBatch(latencies);
  }
}

co20::Task DashboardEngine::SamplerTask(co20::Coroutine &c) {
  SampleMetrics();
  while (!IsStopping()) {
    co_await c.Sleep(std::chrono::milliseconds(100));
    Drain(c.GetInterruptFd());
    if (!IsStopping()) {
      SampleMetrics();
    }
  }
  JoinWorkers();
  SampleMetrics();
  co_return;
}

DashboardEngine::RateSnapshot DashboardEngine::Rate() const {
  std::lock_guard lock(rate_mutex_);
  return {.setting = rate_.Current(), .index = rate_.Index()};
}

DashboardEngine::SlotSnapshot DashboardEngine::Slots() const {
  std::lock_guard lock(rate_mutex_);
  return {.count = slots_.Current(), .index = slots_.Index()};
}

DashboardSnapshot DashboardEngine::Snapshot() const {
  int32_t requested_slots;
  ChannelProfile active_channel;
  {
    std::lock_guard lock(rate_mutex_);
    requested_slots = slots_.Current();
    active_channel = active_channel_;
  }
  std::lock_guard lock(metrics_mutex_);
  DashboardSnapshot snapshot = metrics_.Snapshot();
  snapshot.requested_slots = requested_slots;
  snapshot.active_slots = active_channel.slots;
  snapshot.active_channel = active_channel.name;
  return snapshot;
}

void DashboardEngine::NotifyRateChanged() {
  rate_changed_.notify_all();
  Wake(publisher_control_[1]);
}

void DashboardEngine::NotifyChannelChanged() {
  rate_changed_.notify_all();
  Wake(publisher_control_[1]);
}

void DashboardEngine::ReportError(const absl::Status &status) {
  {
    std::lock_guard lock(metrics_mutex_);
    metrics_.SetError(status.ToString());
    metrics_.RecordWarning("ERROR: " + status.ToString());
  }
  RequestStop();
}

void DashboardEngine::JoinWorkers() {
  if (publisher_thread_.joinable()) {
    publisher_thread_.join();
  }
  if (subscriber_thread_.joinable()) {
    subscriber_thread_.join();
  }
}

void DashboardEngine::RecordPublished(uint64_t count) {
  std::lock_guard lock(metrics_mutex_);
  metrics_.RecordPublished(count);
}

void DashboardEngine::RecordReceivedBatch(
    const std::vector<uint64_t> &latencies) {
  if (latencies.empty()) {
    return;
  }
  std::lock_guard lock(metrics_mutex_);
  for (uint64_t latency : latencies) {
    metrics_.RecordReceived(latency);
  }
}

void DashboardEngine::SampleMetrics() {
  std::lock_guard lock(metrics_mutex_);
  if (dropped_since_warning_ != 0) {
    metrics_.RecordWarning(absl::StrFormat(
        "Dropped %d messages on channel %s", dropped_since_warning_,
        config_.channel));
    dropped_since_warning_ = 0;
  }
  metrics_.Sample(toolbelt::Now());
}

absl::Status DashboardEngine::CreateNonBlockingPipe(int pipe_fds[2]) {
  if (pipe(pipe_fds) != 0) {
    return absl::ErrnoToStatus(errno, "failed to create dashboard pipe");
  }
  for (int fd : {pipe_fds[0], pipe_fds[1]}) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
      const int saved_errno = errno;
      ClosePipe(pipe_fds);
      return absl::ErrnoToStatus(saved_errno,
                                 "failed to make dashboard pipe nonblocking");
    }
  }
  return absl::OkStatus();
}

void DashboardEngine::Wake(int write_fd) {
  if (write_fd == -1) {
    return;
  }
  const char byte = 1;
  (void)write(write_fd, &byte, sizeof(byte));
}

void DashboardEngine::Drain(int read_fd) {
  if (read_fd == -1) {
    return;
  }
  char buffer[32];
  while (read(read_fd, buffer, sizeof(buffer)) > 0) {
  }
}

void DashboardEngine::ClosePipe(int pipe_fds[2]) {
  if (pipe_fds[0] != -1) {
    close(pipe_fds[0]);
    pipe_fds[0] = -1;
  }
  if (pipe_fds[1] != -1) {
    close(pipe_fds[1]);
    pipe_fds[1] = -1;
  }
}

} // namespace subspace::dashboard
