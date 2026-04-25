#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <memory>
#include <vector>

namespace rollingraft {

/**
 * Simple Prometheus-style Counter.
 * Thread-safe, monotonically increasing.
 */
class Counter {
 public:
  void Increment(uint64_t delta = 1) { value_.fetch_add(delta); }

  uint64_t GetValue() const { return value_.load(); }

 private:
  std::atomic<uint64_t> value_{0};
};

/**
 * Simple Prometheus-style Gauge.
 * Thread-safe, can go up and down.
 */
class Gauge {
 public:
  void Set(double value) {
    uint64_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    value_.store(bits);
  }

  void Increment(double delta = 1.0) {
    double current = GetValue();
    Set(current + delta);
  }

  void Decrement(double delta = 1.0) {
    double current = GetValue();
    Set(current - delta);
  }

  double GetValue() const {
    uint64_t bits = value_.load();
    double value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
  }

 private:
  std::atomic<uint64_t> value_{0};
};

/**
 * Simple Prometheus-style Histogram.
 * Thread-safe bucket counters.
 */
class Histogram {
 public:
  explicit Histogram(std::vector<double> buckets) : buckets_(buckets) {
    for (size_t i = 0; i < buckets_.size() + 1; ++i) {
      counts_.push_back(std::make_unique<std::atomic<uint64_t>>(0));
    }
    sum_.store(0.0);
    count_.store(0);
  }

  void Observe(double value) {
    count_.fetch_add(1);
    sum_.fetch_add(value);
    for (size_t i = 0; i < buckets_.size(); ++i) {
      if (value <= buckets_[i]) {
        counts_[i]->fetch_add(1);
      }
    }
    counts_[buckets_.size()]->fetch_add(1);
  }

  uint64_t GetCount() const { return count_.load(); }

  double GetSum() const { return sum_.load(); }

  const std::vector<double>& GetBuckets() const { return buckets_; }

  uint64_t GetBucketCount(size_t idx) const { return counts_[idx]->load(); }

 private:
  std::vector<double> buckets_;
  std::vector<std::unique_ptr<std::atomic<uint64_t>>> counts_;
  std::atomic<double> sum_{0.0};
  std::atomic<uint64_t> count_{0};
};

/**
 * MetricsRegistry collects metrics and formats them as Prometheus text.
 */
class MetricsRegistry {
 public:
  struct MetricKey {
    std::string name;
    std::map<std::string, std::string> labels;

    bool operator<(const MetricKey& other) const {
      if (name != other.name) return name < other.name;
      return labels < other.labels;
    }
  };

  Counter& GetCounter(const std::string& name,
                      const std::map<std::string, std::string>& labels = {});

  Gauge& GetGauge(const std::string& name,
                  const std::map<std::string, std::string>& labels = {});

  Histogram& GetHistogram(const std::string& name,
                          const std::vector<double>& buckets,
                          const std::map<std::string, std::string>& labels =
                              {});

  std::string FormatPrometheus() const;

 private:
  mutable std::mutex mtx_;
  std::map<MetricKey, std::unique_ptr<Counter>> counters_;
  std::map<MetricKey, std::unique_ptr<Gauge>> gauges_;
  std::map<MetricKey, std::unique_ptr<Histogram>> histograms_;
};

}  // namespace rollingraft
