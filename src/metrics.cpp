#include "rollingraft/metrics.h"

#include <sstream>

namespace rollingraft {

Counter& MetricsRegistry::GetCounter(
    const std::string& name, const std::map<std::string, std::string>& labels) {
  std::lock_guard<std::mutex> lock(mtx_);
  MetricKey key{name, labels};
  auto it = counters_.find(key);
  if (it != counters_.end()) {
    return *it->second;
  }
  auto ptr = std::make_unique<Counter>();
  auto& ref = *ptr;
  counters_[key] = std::move(ptr);
  return ref;
}

Gauge& MetricsRegistry::GetGauge(
    const std::string& name, const std::map<std::string, std::string>& labels) {
  std::lock_guard<std::mutex> lock(mtx_);
  MetricKey key{name, labels};
  auto it = gauges_.find(key);
  if (it != gauges_.end()) {
    return *it->second;
  }
  auto ptr = std::make_unique<Gauge>();
  auto& ref = *ptr;
  gauges_[key] = std::move(ptr);
  return ref;
}

Histogram& MetricsRegistry::GetHistogram(
    const std::string& name, const std::vector<double>& buckets,
    const std::map<std::string, std::string>& labels) {
  std::lock_guard<std::mutex> lock(mtx_);
  MetricKey key{name, labels};
  auto it = histograms_.find(key);
  if (it != histograms_.end()) {
    return *it->second;
  }
  auto ptr = std::make_unique<Histogram>(buckets);
  auto& ref = *ptr;
  histograms_[key] = std::move(ptr);
  return ref;
}

void MetricsRegistry::RemoveCounter(
    const std::string& name,
    const std::map<std::string, std::string>& labels) {
  std::lock_guard<std::mutex> lock(mtx_);
  counters_.erase(MetricKey{name, labels});
}

void MetricsRegistry::RemoveGauge(
    const std::string& name,
    const std::map<std::string, std::string>& labels) {
  std::lock_guard<std::mutex> lock(mtx_);
  gauges_.erase(MetricKey{name, labels});
}

void MetricsRegistry::RemoveHistogram(
    const std::string& name,
    const std::map<std::string, std::string>& labels) {
  std::lock_guard<std::mutex> lock(mtx_);
  histograms_.erase(MetricKey{name, labels});
}

static std::string FormatLabels(
    const std::map<std::string, std::string>& labels) {
  if (labels.empty()) return "";
  std::ostringstream oss;
  oss << "{";
  bool first = true;
  for (const auto& [k, v] : labels) {
    if (!first) oss << ",";
    first = false;
    oss << k << "=\"" << v << "\"";
  }
  oss << "}";
  return oss.str();
}

std::string MetricsRegistry::FormatPrometheus() const {
  std::lock_guard<std::mutex> lock(mtx_);
  std::ostringstream oss;

  for (const auto& [key, counter] : counters_) {
    oss << "# TYPE " << key.name << " counter\n";
    oss << key.name << FormatLabels(key.labels) << " " << counter->GetValue()
        << "\n";
  }

  for (const auto& [key, gauge] : gauges_) {
    oss << "# TYPE " << key.name << " gauge\n";
    oss << key.name << FormatLabels(key.labels) << " " << gauge->GetValue()
        << "\n";
  }

  for (const auto& [key, hist] : histograms_) {
    oss << "# TYPE " << key.name << " histogram\n";
    std::string lbl = FormatLabels(key.labels);
    for (size_t i = 0; i < hist->GetBuckets().size(); ++i) {
      oss << key.name << "_bucket{le=\"" << hist->GetBuckets()[i] << "\"";
      if (!lbl.empty()) {
        oss << "," << lbl.substr(1, lbl.size() - 2);
      }
      oss << "} " << hist->GetBucketCount(i) << "\n";
    }
    oss << key.name << "_bucket{le=\"+Inf\"";
    if (!lbl.empty()) {
      oss << "," << lbl.substr(1, lbl.size() - 2);
    }
    oss << "} " << hist->GetBucketCount(hist->GetBuckets().size()) << "\n";
    oss << key.name << "_sum" << lbl << " " << hist->GetSum() << "\n";
    oss << key.name << "_count" << lbl << " " << hist->GetCount() << "\n";
  }

  return oss.str();
}

}  // namespace rollingraft
