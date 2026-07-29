/**
 * @file protobuf_mvp_benchmark.cpp
 * @brief MVP benchmark: JSON+Base64 vs Protobuf serialization
 *
 * Compares serialization/deserialization performance and message size
 * for RaftLogEntry using current JSON+Base64 vs proposed Protobuf.
 */

#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "rollingraft/raft_log.h"

#include "raft_log_entry.pb.h"
#include <nlohmann/json.hpp>

using json = nlohmann::json;
using namespace rollingraft;

// ------------------------------------------------------------------
// Base64 helpers (copied from wal_persister.cpp)
// ------------------------------------------------------------------

static const char kBase64Chars[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static std::string Base64Encode(const std::string& input) {
  std::string encoded;
  encoded.reserve(((input.size() + 2) / 3) * 4);
  size_t i = 0;
  uint8_t array3[3];
  uint8_t array4[4];
  int in_len = static_cast<int>(input.size());
  while (in_len--) {
    array3[i++] = static_cast<uint8_t>(input[input.size() - in_len - 1]);
    if (i == 3) {
      array4[0] = (array3[0] & 0xfc) >> 2;
      array4[1] = ((array3[0] & 0x03) << 4) + ((array3[1] & 0xf0) >> 4);
      array4[2] = ((array3[1] & 0x0f) << 2) + ((array3[2] & 0xc0) >> 6);
      array4[3] = array3[2] & 0x3f;
      for (int j = 0; j < 4; ++j) {
        encoded += kBase64Chars[array4[j]];
      }
      i = 0;
    }
  }
  if (i) {
    for (int j = i; j < 3; ++j) {
      array3[j] = '\0';
    }
    array4[0] = (array3[0] & 0xfc) >> 2;
    array4[1] = ((array3[0] & 0x03) << 4) + ((array3[1] & 0xf0) >> 4);
    array4[2] = ((array3[1] & 0x0f) << 2) + ((array3[2] & 0xc0) >> 6);
    array4[3] = array3[2] & 0x3f;
    for (int j = 0; j < (i + 1); ++j) {
      encoded += kBase64Chars[array4[j]];
    }
    while (i++ < 3) {
      encoded += '=';
    }
  }
  return encoded;
}

static std::string Base64Decode(const std::string& encoded) {
  if (encoded.empty()) {
    return std::string();
  }
  std::string decoded;
  decoded.reserve((encoded.size() / 4) * 3);
  auto lookup = [](char c) -> int {
    if (c >= 'A' && c <= 'Z') {
      return c - 'A';
    }
    if (c >= 'a' && c <= 'z') {
      return c - 'a' + 26;
    }
    if (c >= '0' && c <= '9') {
      return c - '0' + 52;
    }
    if (c == '+') {
      return 62;
    }
    if (c == '/') {
      return 63;
    }
    return -1;
  };
  size_t in_len = encoded.size();
  int i = 0, j = 0, in_ = 0;
  uint8_t array4[4], array3[3];
  while (in_len-- && encoded[in_] != '=') {
    int val = lookup(encoded[in_]);
    if (val == -1) {
      in_++;
      continue;
    }
    array4[i++] = static_cast<uint8_t>(val);
    in_++;
    if (i == 4) {
      array3[0] = (array4[0] << 2) + ((array4[1] & 0x30) >> 4);
      array3[1] = ((array4[1] & 0x0f) << 2) + ((array4[2] & 0x3c) >> 2);
      array3[2] = ((array4[2] & 0x03) << 6) + array4[3];
      for (j = 0; j < 3; j++) {
        decoded += array3[j];
      }
      i = 0;
    }
  }
  if (i) {
    for (j = i; j < 4; j++) {
      array4[j] = 0;
    }
    array3[0] = (array4[0] << 2) + ((array4[1] & 0x30) >> 4);
    array3[1] = ((array4[1] & 0x0f) << 2) + ((array4[2] & 0x3c) >> 2);
    array3[2] = ((array4[2] & 0x03) << 6) + array4[3];
    for (j = 0; j < (i - 1); j++) {
      decoded += array3[j];
    }
  }
  return decoded;
}

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

static std::string RandomString(size_t len, std::mt19937& rng) {
  static const char kChars[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::uniform_int_distribution<size_t> dist(0, sizeof(kChars) - 2);
  std::string s;
  s.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    s.push_back(kChars[dist(rng)]);
  }
  return s;
}

static RaftLogEntry MakeEntry(uint64_t index, uint64_t term, const std::string& data,
                              const std::string& command) {
  RaftLogEntry e;
  e.index_ = index;
  e.term_ = term;
  e.data_ = data;
  e.command_ = command;
  e.checksum_ = 0xDEADBEEF;
  return e;
}

// ------------------------------------------------------------------
// JSON+Base64 serialization (current)
// ------------------------------------------------------------------

static std::string SerializeJson(const RaftLogEntry& entry) {
  json j;
  j["index"] = static_cast<uint64_t>(entry.index_);
  j["term"] = static_cast<uint64_t>(entry.term_);
  j["data"] = Base64Encode(entry.data_);
  j["command"] = Base64Encode(entry.command_);
  j["checksum"] = entry.checksum_;
  return j.dump();
}

static bool DeserializeJson(const std::string& payload, RaftLogEntry& entry) {
  try {
    json j = json::parse(payload);
    entry.index_ = j["index"].get<uint64_t>();
    entry.term_ = j["term"].get<uint64_t>();
    entry.data_ = Base64Decode(j["data"].get<std::string>());
    entry.command_ = Base64Decode(j.value("command", std::string()));
    entry.checksum_ = j.value("checksum", 0);
    return true;
  } catch (...) {
    return false;
  }
}

// ------------------------------------------------------------------
// Protobuf serialization (proposed)
// ------------------------------------------------------------------

static std::string SerializeProtobuf(const RaftLogEntry& entry) {
  RaftLogEntryProto proto;
  proto.set_index(entry.index_);
  proto.set_term(entry.term_);
  proto.set_data(entry.data_);
  proto.set_command(entry.command_);
  proto.set_checksum(entry.checksum_);
  return proto.SerializeAsString();
}

static bool DeserializeProtobuf(const std::string& payload, RaftLogEntry& entry) {
  RaftLogEntryProto proto;
  if (!proto.ParseFromString(payload)) {
    return false;
  }
  entry.index_ = proto.index();
  entry.term_ = proto.term();
  entry.data_ = proto.data();
  entry.command_ = proto.command();
  entry.checksum_ = proto.checksum();
  return true;
}

// ------------------------------------------------------------------
// Benchmark
// ------------------------------------------------------------------

struct BenchmarkResult {
  size_t payload_size = 0;
  double serialize_ns = 0.0;
  double deserialize_ns = 0.0;
  size_t serialized_bytes = 0;
};

static BenchmarkResult RunBenchmark(
    const std::string& name, const std::vector<RaftLogEntry>& entries,
    std::function<std::string(const RaftLogEntry&)> serialize,
    std::function<bool(const std::string&, RaftLogEntry&)> deserialize) {
  const int kIterations = 100000;

  // Warmup
  for (int i = 0; i < 1000; ++i) {
    auto s = serialize(entries[i % entries.size()]);
    RaftLogEntry tmp;
    deserialize(s, tmp);
  }

  // Serialize benchmark
  std::vector<std::string> serialized;
  serialized.reserve(entries.size());
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < kIterations; ++i) {
    serialized.push_back(serialize(entries[i % entries.size()]));
  }
  auto t1 = std::chrono::steady_clock::now();
  double serialize_ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / kIterations;

  // Deserialize benchmark
  std::vector<RaftLogEntry> deserialized;
  deserialized.reserve(serialized.size());
  auto t2 = std::chrono::steady_clock::now();
  for (const auto& s : serialized) {
    RaftLogEntry tmp;
    deserialize(s, tmp);
    deserialized.push_back(tmp);
  }
  auto t3 = std::chrono::steady_clock::now();
  double deserialize_ns =
      std::chrono::duration<double, std::nano>(t3 - t2).count() / serialized.size();

  size_t total_bytes = 0;
  for (const auto& s : serialized) {
    total_bytes += s.size();
  }

  BenchmarkResult result;
  result.payload_size = entries[0].data_.size();
  result.serialize_ns = serialize_ns;
  result.deserialize_ns = deserialize_ns;
  result.serialized_bytes = total_bytes / serialized.size();

  std::cout << "[" << name << "] payload=" << result.payload_size << "B"
            << " serialize=" << std::fixed << std::setprecision(1) << result.serialize_ns << "ns"
            << " deserialize=" << result.deserialize_ns << "ns"
            << " size=" << result.serialized_bytes << "B"
            << "\n";

  return result;
}

int main() {
  std::cout << "Protobuf MVP Benchmark: JSON+Base64 vs Protobuf\n";
  std::cout << "=================================================\n\n";

  std::mt19937 rng(42);

  std::vector<int> payload_sizes = {100, 1024, 10240};

  for (int payload_bytes : payload_sizes) {
    std::cout << "--- Payload: " << payload_bytes << " B ---\n";

    // Generate test entries
    std::vector<RaftLogEntry> entries;
    entries.reserve(100);
    for (int i = 0; i < 100; ++i) {
      std::string data = RandomString(payload_bytes, rng);
      std::string command = RandomString(payload_bytes / 2, rng);
      entries.push_back(MakeEntry(i + 1, 1, data, command));
    }

    auto json_result = RunBenchmark("JSON+Base64", entries, SerializeJson, DeserializeJson);
    auto pb_result = RunBenchmark("Protobuf   ", entries, SerializeProtobuf, DeserializeProtobuf);

    double serialize_speedup = json_result.serialize_ns / pb_result.serialize_ns;
    double deserialize_speedup = json_result.deserialize_ns / pb_result.deserialize_ns;
    double size_reduction =
        static_cast<double>(json_result.serialized_bytes) / pb_result.serialized_bytes;

    std::cout << "Speedup: serialize=" << std::setprecision(2) << serialize_speedup << "x"
              << " deserialize=" << deserialize_speedup << "x"
              << " size=" << size_reduction << "x smaller\n\n";
  }

  return 0;
}
