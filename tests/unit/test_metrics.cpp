#include <thread>
#include <vector>

#include "rollingraft/metrics.h"

#include <gtest/gtest.h>

using namespace rollingraft;

TEST(MetricsTest, CounterIncrement) {
  MetricsRegistry registry;
  auto& counter = registry.GetCounter("test_counter");
  EXPECT_EQ(counter.GetValue(), 0);
  counter.Increment();
  EXPECT_EQ(counter.GetValue(), 1);
  counter.Increment(5);
  EXPECT_EQ(counter.GetValue(), 6);
}

TEST(MetricsTest, CounterWithLabels) {
  MetricsRegistry registry;
  auto& c1 = registry.GetCounter("requests", {{"method", "GET"}});
  auto& c2 = registry.GetCounter("requests", {{"method", "POST"}});
  c1.Increment();
  c2.Increment(3);
  EXPECT_EQ(c1.GetValue(), 1);
  EXPECT_EQ(c2.GetValue(), 3);
}

TEST(MetricsTest, GaugeSetAndInc) {
  MetricsRegistry registry;
  auto& gauge = registry.GetGauge("temperature");
  gauge.Set(36.5);
  EXPECT_DOUBLE_EQ(gauge.GetValue(), 36.5);
  gauge.Increment(0.5);
  EXPECT_DOUBLE_EQ(gauge.GetValue(), 37.0);
  gauge.Decrement(1.0);
  EXPECT_DOUBLE_EQ(gauge.GetValue(), 36.0);
}

TEST(MetricsTest, HistogramObserve) {
  MetricsRegistry registry;
  std::vector<double> buckets = {1, 10, 100};
  auto& hist = registry.GetHistogram("latency_ms", buckets);

  hist.Observe(0.5);
  hist.Observe(5);
  hist.Observe(50);
  hist.Observe(200);

  EXPECT_EQ(hist.GetCount(), 4);
  EXPECT_DOUBLE_EQ(hist.GetSum(), 255.5);
  EXPECT_EQ(hist.GetBucketCount(0), 1);  // <= 1
  EXPECT_EQ(hist.GetBucketCount(1), 2);  // <= 10
  EXPECT_EQ(hist.GetBucketCount(2), 3);  // <= 100
  EXPECT_EQ(hist.GetBucketCount(3), 4);  // <= +Inf
}

TEST(MetricsTest, FormatPrometheus) {
  MetricsRegistry registry;
  registry.GetCounter("requests", {{"method", "GET"}}).Increment(10);
  registry.GetGauge("temperature").Set(36.5);

  std::string output = registry.FormatPrometheus();
  EXPECT_NE(output.find("requests{method=\"GET\"} 10"), std::string::npos);
  EXPECT_NE(output.find("temperature 36.5"), std::string::npos);
  EXPECT_NE(output.find("# TYPE requests counter"), std::string::npos);
  EXPECT_NE(output.find("# TYPE temperature gauge"), std::string::npos);
}

TEST(MetricsTest, ThreadSafetyStress) {
  MetricsRegistry registry;
  auto& counter = registry.GetCounter("stress_counter");

  std::vector<std::thread> threads;
  for (int i = 0; i < 8; ++i) {
    threads.emplace_back([&counter]() {
      for (int j = 0; j < 1000; ++j) {
        counter.Increment();
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(counter.GetValue(), 8000);
}

TEST(MetricsTest, RemoveGauge) {
  MetricsRegistry registry;
  registry.GetGauge("temperature", {{"room", "kitchen"}}).Set(25.0);
  registry.GetGauge("temperature", {{"room", "bedroom"}}).Set(20.0);

  std::string output = registry.FormatPrometheus();
  EXPECT_NE(output.find("temperature{room=\"kitchen\"} 25"), std::string::npos);
  EXPECT_NE(output.find("temperature{room=\"bedroom\"} 20"), std::string::npos);

  registry.RemoveGauge("temperature", {{"room", "kitchen"}});
  output = registry.FormatPrometheus();
  EXPECT_EQ(output.find("temperature{room=\"kitchen\"}"), std::string::npos);
  EXPECT_NE(output.find("temperature{room=\"bedroom\"} 20"), std::string::npos);
}
