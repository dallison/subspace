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

uint64_t ChannelGeneration(uint64_t slots_generation,
                           uint64_t queue_generation,
                           uint64_t mode_generation) {
  return (mode_generation << 48) | (slots_generation << 32) | queue_generation;
}

bool UsesReliableTransport(PerformanceMode mode) {
  return mode == PerformanceMode::kReliable;
}

bool UsesBatchSubscriberDrain(PerformanceMode mode) {
  return mode == PerformanceMode::kThroughput ||
         mode == PerformanceMode::kReliable;
}

uint64_t SubscriberQueueArenaBytes(int32_t queue_size) {
  if (queue_size <= 0) {
    return 0;
  }
  return subspace::SlotQueueBlockSize(static_cast<size_t>(queue_size));
}

} // namespace

DashboardEngine::ChannelProfile DashboardEngine::CurrentChannel() const {
  const int32_t slots = slots_.Current();
  const int32_t queue_size = queues_.Current();
  const PerformanceMode mode = mode_.Current().mode;
  return {
      .name = ChannelName(config_.channel, slots, queue_size,
                          UsesReliableTransport(mode)),
      .slots = slots,
      .queue_size = queue_size,
      .generation = ChannelGeneration(slots_.Generation(), queues_.Generation(),
                                      mode_.Generation()),
  };
}

DashboardEngine::DashboardEngine(Config config) : config_(std::move(config)) {
  published_channel_ = CurrentChannel();
  active_channel_ = published_channel_;
  metrics_.SetTargetRate(rate_.Current().label);
  metrics_.ResetForChannel(published_channel_.name, published_channel_.slots,
                            published_channel_.queue_size);
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

bool DashboardEngine::IncreaseMode() {
  PerformanceMode mode;
  const char *label;
  {
    std::lock_guard lock(rate_mutex_);
    if (!mode_.Increase()) {
      return false;
    }
    mode = mode_.Current().mode;
    label = mode_.Current().label;
  }
  performance_mode_.store(mode, std::memory_order_release);
  ResetMeasurements();
  RecordWarning(absl::StrFormat("Switched to %s mode", label));
  NotifyModeChanged();
  return true;
}

bool DashboardEngine::DecreaseMode() {
  PerformanceMode mode;
  const char *label;
  {
    std::lock_guard lock(rate_mutex_);
    if (!mode_.Decrease()) {
      return false;
    }
    mode = mode_.Current().mode;
    label = mode_.Current().label;
  }
  performance_mode_.store(mode, std::memory_order_release);
  ResetMeasurements();
  RecordWarning(absl::StrFormat("Switched to %s mode", label));
  NotifyModeChanged();
  return true;
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

void DashboardEngine::ResetMeasurements() {
  std::lock_guard lock(metrics_mutex_);
  metrics_.ResetStatistics();
  dropped_since_warning_ = 0;
}

bool DashboardEngine::IncreaseSlots() {
  {
    std::lock_guard lock(rate_mutex_);
    if (!slots_.Increase()) {
      return false;
    }
  }
  ResetMeasurements();
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
  ResetMeasurements();
  NotifyChannelChanged();
  return true;
}

bool DashboardEngine::IncreaseSubscriberQueue() {
  {
    std::lock_guard lock(rate_mutex_);
    if (!queues_.Increase()) {
      return false;
    }
  }
  ResetMeasurements();
  NotifyChannelChanged();
  return true;
}

bool DashboardEngine::DecreaseSubscriberQueue() {
  {
    std::lock_guard lock(rate_mutex_);
    if (!queues_.Decrease()) {
      return false;
    }
  }
  ResetMeasurements();
  NotifyChannelChanged();
  return true;
}

void DashboardEngine::RecordWarning(std::string warning) {
  std::lock_guard lock(metrics_mutex_);
  metrics_.RecordWarning(std::move(warning));
}

absl::StatusOr<subspace::Publisher>
DashboardEngine::CreatePublisher(const ChannelProfile &profile) {
  PerformanceMode mode;
  {
    std::lock_guard lock(rate_mutex_);
    mode = mode_.Current().mode;
  }
  subspace::PublisherOptions options =
      subspace::PublisherOptions()
          .SetFixedSize(true)
          .SetType("subspace.dashboard.Timestamp")
          .SetSubscriberQueueArenaSize(
              SubscriberQueueArenaBytes(profile.queue_size));
  if (UsesReliableTransport(mode)) {
    options.SetReliable(true);
  }
  return publisher_client_->CreatePublisher(profile.name, kPayloadSize,
                                           profile.slots, options);
}

absl::StatusOr<subspace::Subscriber>
DashboardEngine::CreateSubscriber(const ChannelProfile &profile) {
  PerformanceMode mode;
  {
    std::lock_guard lock(rate_mutex_);
    mode = mode_.Current().mode;
  }
  subspace::SubscriberOptions options;
  options.SetType("subspace.dashboard.Timestamp");
  options.SetLogDroppedMessages(false);
  options.SetSubscriberQueueSize(profile.queue_size);
  if (UsesReliableTransport(mode)) {
    options.SetReliable(true);
  }
  // The dashboard's subscriber_busy_ handoff guarantees that the publisher
  // wakes an idle subscriber. Avoid an additional trigger write when this
  // thread releases each message.
  options.SetMaxActiveMessages(8);
  options.SetTriggerOnRelease(false);
  auto subscriber =
      subscriber_client_->CreateSubscriber(profile.name, options);
  if (!subscriber.ok()) {
    return subscriber.status();
  }
  auto callback_status = subscriber->RegisterDroppedMessageCallback(
      [this](subspace::Subscriber *, int64_t count) {
        if (count > 0) {
          RecordDropped(static_cast<uint64_t>(count));
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
  uint64_t mode_generation;
  uint64_t channel_generation;
  size_t publish_batch_size;
  {
    std::lock_guard lock(rate_mutex_);
    generation = rate_.Generation();
    mode_generation = mode_.Generation();
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
    uint64_t current_mode_generation;
    PerformanceMode mode;
    uint64_t requested_generation;
    {
      std::lock_guard lock(rate_mutex_);
      setting = rate_.Current();
      current_generation = rate_.Generation();
      current_mode_generation = mode_.Generation();
      mode = mode_.Current().mode;
      requested_generation = ChannelGeneration(
          slots_.Generation(), queues_.Generation(), mode_.Generation());
    }
    mode_generation = current_mode_generation;
    if (channel_generation != requested_generation) {
      ChannelProfile requested_channel;
      {
        std::lock_guard lock(rate_mutex_);
        requested_channel = CurrentChannel();
      }
      record_published();
      auto publisher = CreatePublisher(requested_channel);
      if (!publisher.ok()) {
        std::lock_guard lock(rate_mutex_);
        if (CurrentChannel().generation != requested_channel.generation) {
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
      RecordWarning(absl::StrFormat(
          "Publisher switched to %s (%d slots, queue %d)",
          requested_channel.name, requested_channel.slots,
          requested_channel.queue_size));
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
               mode_.Generation() != mode_generation ||
               ChannelGeneration(slots_.Generation(), queues_.Generation(),
                                 mode_.Generation()) != channel_generation;
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
                     mode_.Generation() != mode_generation ||
                     ChannelGeneration(slots_.Generation(),
                                       queues_.Generation(),
                                       mode_.Generation()) !=
                         channel_generation;
            });
      } else {
        // sched_yield() hands this core to the UI thread and the subscriber.
        // At 100 kHz and above the remaining wait is under 100 us, so spin
        // until the deadline instead of leaving the core.
        while (!IsStopping() && toolbelt::Now() < deadline_ns) {
        }
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
    // The subscriber polls only when it has caught up. While it is still
    // draining, another trigger write is a syscall with nobody waiting on it.
    // Recheck after the publish so a subscriber that fell asleep mid-publish
    // is still woken.
    const bool notify =
        mode == PerformanceMode::kLatency ||
        mode == PerformanceMode::kReliable ||
        !subscriber_busy_.load(std::memory_order_acquire);
    auto published = publisher_->PublishMessage(kPayloadSize, notify);
    if (published.ok() && mode == PerformanceMode::kThroughput && !notify &&
        !subscriber_busy_.load(std::memory_order_acquire)) {
      publisher_->NotifySubscribers();
    }
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
  std::optional<uint64_t> latency_mode_ordinal;

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
        latency_mode_ordinal.reset();
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
                                   published_channel.slots,
                                   published_channel.queue_size);
        }
        RecordWarning(absl::StrFormat(
            "Subscriber switched to %s (%d slots, queue %d)",
            published_channel.name, published_channel.slots,
            published_channel.queue_size));
        continue;
      }
    }
    if ((fds[0].revents & (POLLIN | POLLERR | POLLHUP)) == 0) {
      continue;
    }

    if (performance_mode_.load(std::memory_order_acquire) ==
        PerformanceMode::kLatency) {
      auto message = subscriber_->ReadMessage(subspace::ReadMode::kReadNewest);
      if (!message.ok()) {
        ReportError(message.status());
        return;
      }
      if (message->length == 0) {
        continue;
      }
      if (message->length != kPayloadSize || message->buffer == nullptr) {
        ReportError(absl::DataLossError(absl::StrFormat(
            "channel %s received a %d-byte payload; expected %d", channel_name,
            message->length, kPayloadSize)));
        return;
      }
      uint64_t dropped = 0;
      if (latency_mode_ordinal.has_value() &&
          message->ordinal > *latency_mode_ordinal &&
          message->ordinal - *latency_mode_ordinal > 1) {
        dropped = message->ordinal - *latency_mode_ordinal - 1;
      }
      latency_mode_ordinal = message->ordinal;

      uint64_t publish_time_ns = 0;
      std::memcpy(&publish_time_ns, message->buffer, sizeof(publish_time_ns));
      const uint64_t receive_time_ns = toolbelt::Now();
      RecordReceived(receive_time_ns >= publish_time_ns
                         ? receive_time_ns - publish_time_ns
                         : 0,
                     dropped);
      continue;
    }

    latency_mode_ordinal.reset();
    // Keep draining after clearing subscriber_busy_. A publish that finished
    // while this thread was still marked busy skipped its trigger write, and
    // poll() would then sleep through a message that is already readable.
    for (;;) {
      subscriber_busy_.store(true, std::memory_order_release);
      latencies.clear();
      bool first_message = true;
      bool saw_message = false;
      absl::Status read_error = absl::OkStatus();
      for (;;) {
        auto message = first_message
                           ? subscriber_->ReadMessage()
                           : subscriber_->ReadMessageFromBatch();
        first_message = false;
        if (!message.ok()) {
          read_error = message.status();
          break;
        }
        if (message->length == 0) {
          break;
        }
        if (message->length != kPayloadSize || message->buffer == nullptr) {
          read_error = absl::DataLossError(absl::StrFormat(
              "channel %s received a %d-byte payload; expected %d",
              channel_name, message->length, kPayloadSize));
          break;
        }

        uint64_t publish_time_ns = 0;
        std::memcpy(&publish_time_ns, message->buffer, sizeof(publish_time_ns));
        const uint64_t receive_time_ns = toolbelt::Now();
        latencies.push_back(receive_time_ns >= publish_time_ns
                                ? receive_time_ns - publish_time_ns
                                : 0);
        saw_message = true;
      }
      RecordReceivedBatch(latencies);
      subscriber_busy_.store(false, std::memory_order_release);
      if (!read_error.ok()) {
        ReportError(read_error);
        return;
      }
      if (!saw_message) {
        break;
      }
      if (!UsesBatchSubscriberDrain(
              performance_mode_.load(std::memory_order_acquire))) {
        break;
      }
      auto missed = subscriber_->ReadMessage();
      if (!missed.ok()) {
        ReportError(missed.status());
        return;
      }
      if (missed->length == 0) {
        break;
      }
      if (missed->length != kPayloadSize || missed->buffer == nullptr) {
        ReportError(absl::DataLossError(absl::StrFormat(
            "channel %s received a %d-byte payload; expected %d", channel_name,
            missed->length, kPayloadSize)));
        return;
      }
      uint64_t publish_time_ns = 0;
      std::memcpy(&publish_time_ns, missed->buffer, sizeof(publish_time_ns));
      const uint64_t receive_time_ns = toolbelt::Now();
      latencies.clear();
      latencies.push_back(receive_time_ns >= publish_time_ns
                              ? receive_time_ns - publish_time_ns
                              : 0);
      RecordReceivedBatch(latencies);
    }
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

DashboardEngine::ModeSnapshot DashboardEngine::Mode() const {
  std::lock_guard lock(rate_mutex_);
  return {.setting = mode_.Current(), .index = mode_.Index()};
}

DashboardEngine::RateSnapshot DashboardEngine::Rate() const {
  std::lock_guard lock(rate_mutex_);
  return {.setting = rate_.Current(), .index = rate_.Index()};
}

DashboardEngine::SlotSnapshot DashboardEngine::Slots() const {
  std::lock_guard lock(rate_mutex_);
  return {.count = slots_.Current(), .index = slots_.Index()};
}

DashboardEngine::QueueSnapshot DashboardEngine::SubscriberQueue() const {
  std::lock_guard lock(rate_mutex_);
  return {.size = queues_.Current(), .index = queues_.Index()};
}

DashboardSnapshot DashboardEngine::Snapshot() const {
  int32_t requested_slots;
  int32_t requested_queue_size;
  ChannelProfile active_channel;
  {
    std::lock_guard lock(rate_mutex_);
    requested_slots = slots_.Current();
    requested_queue_size = queues_.Current();
    active_channel = active_channel_;
  }
  std::lock_guard lock(metrics_mutex_);
  DashboardSnapshot snapshot = metrics_.Snapshot();
  snapshot.requested_slots = requested_slots;
  snapshot.active_slots = active_channel.slots;
  snapshot.requested_queue_size = requested_queue_size;
  snapshot.active_queue_size = active_channel.queue_size;
  snapshot.active_channel = active_channel.name;
  return snapshot;
}

void DashboardEngine::NotifyRateChanged() {
  rate_changed_.notify_all();
  Wake(publisher_control_[1]);
}

void DashboardEngine::NotifyModeChanged() {
  rate_changed_.notify_all();
  Wake(publisher_control_[1]);
  Wake(subscriber_stop_[1]);
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

void DashboardEngine::RecordDropped(uint64_t count) {
  std::lock_guard lock(metrics_mutex_);
  metrics_.RecordDropped(count);
  dropped_since_warning_ += count;
}

void DashboardEngine::RecordReceived(uint64_t latency_ns, uint64_t dropped) {
  std::lock_guard lock(metrics_mutex_);
  metrics_.RecordReceived(latency_ns);
  if (dropped != 0) {
    metrics_.RecordDropped(dropped);
    dropped_since_warning_ += dropped;
  }
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
