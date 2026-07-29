/**
 * @file state_persister.cpp
 * @brief LevelDB-backed metadata and snapshot persister
 */

#include "rollingraft/state_persister.h"

#include <cstring>

#include "rollingraft/logger.h"

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

namespace rollingraft {

// ==================== SHA-256 Implementation ====================

static const uint32_t kSha256K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

static inline uint32_t Sha256Ror(uint32_t x, uint32_t n) { return (x >> n) | (x << (32 - n)); }

static inline void Sha256CompressRound(uint32_t& a, uint32_t& b, uint32_t& c, uint32_t& d,
                                       uint32_t& e, uint32_t& f, uint32_t& g, uint32_t& h,
                                       uint32_t k, uint32_t w) {
  uint32_t s1 = Sha256Ror(e, 6) ^ Sha256Ror(e, 11) ^ Sha256Ror(e, 25);
  uint32_t ch = (e & f) ^ (~e & g);
  uint32_t temp1 = h + s1 + ch + k + w;
  uint32_t s0 = Sha256Ror(a, 2) ^ Sha256Ror(a, 13) ^ Sha256Ror(a, 22);
  uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
  uint32_t temp2 = s0 + maj;

  h = g;
  g = f;
  f = e;
  e = d + temp1;
  d = c;
  c = b;
  b = a;
  a = temp1 + temp2;
}

struct Sha256Context {
  uint32_t h[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                   0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
  uint8_t buffer[64] = {};
  size_t buffer_len = 0;
  uint64_t total_len = 0;
};

static void Sha256ProcessChunk(Sha256Context& ctx, const uint8_t* chunk) {
  uint32_t w[64];
  for (size_t j = 0; j < 16; ++j) {
    w[j] = (static_cast<uint32_t>(chunk[j * 4]) << 24) |
           (static_cast<uint32_t>(chunk[j * 4 + 1]) << 16) |
           (static_cast<uint32_t>(chunk[j * 4 + 2]) << 8) | static_cast<uint32_t>(chunk[j * 4 + 3]);
  }
  for (size_t j = 16; j < 64; ++j) {
    uint32_t s0 = Sha256Ror(w[j - 15], 7) ^ Sha256Ror(w[j - 15], 18) ^ (w[j - 15] >> 3);
    uint32_t s1 = Sha256Ror(w[j - 2], 17) ^ Sha256Ror(w[j - 2], 19) ^ (w[j - 2] >> 10);
    w[j] = w[j - 16] + s0 + w[j - 7] + s1;
  }
  uint32_t a = ctx.h[0], b = ctx.h[1], c = ctx.h[2], d = ctx.h[3];
  uint32_t e = ctx.h[4], f = ctx.h[5], g = ctx.h[6], hh = ctx.h[7];
  for (size_t j = 0; j < 64; ++j) {
    Sha256CompressRound(a, b, c, d, e, f, g, hh, kSha256K[j], w[j]);
  }
  ctx.h[0] += a;
  ctx.h[1] += b;
  ctx.h[2] += c;
  ctx.h[3] += d;
  ctx.h[4] += e;
  ctx.h[5] += f;
  ctx.h[6] += g;
  ctx.h[7] += hh;
}

static void Sha256Init(Sha256Context& ctx) { ctx = Sha256Context{}; }

static void Sha256Update(Sha256Context& ctx, const char* data, size_t len) {
  ctx.total_len += len;
  size_t i = 0;
  if (ctx.buffer_len > 0) {
    size_t to_copy = std::min(len, 64 - ctx.buffer_len);
    std::memcpy(ctx.buffer + ctx.buffer_len, data, to_copy);
    ctx.buffer_len += to_copy;
    i += to_copy;
    if (ctx.buffer_len == 64) {
      Sha256ProcessChunk(ctx, ctx.buffer);
      ctx.buffer_len = 0;
    }
  }
  while (i + 64 <= len) {
    Sha256ProcessChunk(ctx, reinterpret_cast<const uint8_t*>(data + i));
    i += 64;
  }
  if (i < len) {
    size_t remaining = len - i;
    std::memcpy(ctx.buffer + ctx.buffer_len, data + i, remaining);
    ctx.buffer_len += remaining;
  }
}

static void Sha256Final(Sha256Context& ctx, uint8_t* output) {
  uint8_t final_chunk[128] = {};
  std::memcpy(final_chunk, ctx.buffer, ctx.buffer_len);
  final_chunk[ctx.buffer_len] = 0x80;
  size_t final_len = ctx.buffer_len + 1;
  if (final_len > 56) {
    Sha256ProcessChunk(ctx, final_chunk);
    std::memset(final_chunk, 0, 64);
  }
  uint64_t total_bits = ctx.total_len * 8;
  for (size_t j = 0; j < 8; ++j) {
    final_chunk[63 - j] = static_cast<uint8_t>(total_bits >> (j * 8));
  }
  Sha256ProcessChunk(ctx, final_chunk);
  for (size_t i = 0; i < 8; ++i) {
    output[i * 4] = static_cast<uint8_t>(ctx.h[i] >> 24);
    output[i * 4 + 1] = static_cast<uint8_t>(ctx.h[i] >> 16);
    output[i * 4 + 2] = static_cast<uint8_t>(ctx.h[i] >> 8);
    output[i * 4 + 3] = static_cast<uint8_t>(ctx.h[i]);
  }
}

static void ComputeSha256(const char* data, size_t len, uint8_t* output) {
  Sha256Context ctx;
  Sha256Init(ctx);
  Sha256Update(ctx, data, len);
  Sha256Final(ctx, output);
}

// Key prefixes
constexpr char kStateTermKey[] = "state:term";
constexpr char kStateVotedForKey[] = "state:voted_for";
constexpr char kSnapshotDataKey[] = "snapshot:data";
constexpr char kSnapshotMetaKey[] = "snapshot:meta";
constexpr char kSnapshotHashKey[] = "snapshot:hash";
constexpr char kSnapshotChunkPrefix[] = "snapshot:chunk:";
constexpr char kSnapshotTmpChunkPrefix[] = "snapshot:tmp:chunk:";

StatePersister::StatePersister() = default;

StatePersister::~StatePersister() { Close(); }

void StatePersister::SetSyncOnWrite(bool sync) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  sync_on_write_ = sync;
}

void StatePersister::SetCompressionType(Persister::CompressionType type) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  compression_type_ = type;
}

Status StatePersister::Open(const std::string& data_dir) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  if (db_ != nullptr) {
    return Status::Error("StatePersister already open");
  }

  leveldb::Options options;
  options.create_if_missing = true;
  options.compression = (compression_type_ == Persister::kSnappyCompression)
                            ? leveldb::kSnappyCompression
                            : leveldb::kNoCompression;

  leveldb::DB* db_ptr = nullptr;
  leveldb::Status s = leveldb::DB::Open(options, data_dir, &db_ptr);
  if (!s.ok()) {
    return Status::Error("Failed to open LevelDB: " + s.ToString());
  }

  db_.reset(db_ptr);
  LoadStateFromDB();

  return Status::OK();
}

void StatePersister::Close() {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  db_.reset();
}

Status StatePersister::SaveState(const PersistentState& state) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  if (!db_) {
    return Status::Error("StatePersister not open");
  }

  leveldb::WriteBatch batch;
  batch.Put(kStateTermKey, leveldb::Slice(reinterpret_cast<const char*>(&state.current_term),
                                          sizeof(state.current_term)));
  batch.Put(kStateVotedForKey, leveldb::Slice(reinterpret_cast<const char*>(&state.voted_for),
                                              sizeof(state.voted_for)));

  leveldb::WriteOptions write_options;
  write_options.sync = true;
  leveldb::Status s = db_->Write(write_options, &batch);
  if (!s.ok()) {
    return Status::Error("Failed to save state: " + s.ToString());
  }

  cached_state_ = state;
  return Status::OK();
}

Status StatePersister::LoadState(PersistentState& state) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  if (!db_) {
    return Status::Error("StatePersister not open");
  }

  state = cached_state_;
  return Status::OK();
}

Status StatePersister::SaveSnapshot(const std::string& snapshot_data, uint64_t last_index,
                                    uint64_t last_term) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  if (!db_) {
    return Status::Error("StatePersister not open");
  }

  uint8_t hash[32];
  ComputeSha256(snapshot_data.data(), snapshot_data.size(), hash);

  leveldb::WriteBatch batch;
  batch.Put(kSnapshotDataKey, snapshot_data);

  char meta[16];
  std::memcpy(meta, &last_index, sizeof(last_index));
  std::memcpy(meta + 8, &last_term, sizeof(last_term));
  batch.Put(kSnapshotMetaKey, leveldb::Slice(meta, sizeof(meta)));

  batch.Put(kSnapshotHashKey, leveldb::Slice(reinterpret_cast<const char*>(hash), 32));

  leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
  if (!s.ok()) {
    return Status::Error("Failed to save snapshot: " + s.ToString());
  }

  snapshot_last_index_ = last_index;
  snapshot_last_term_ = last_term;

  LOG_INFO("Snapshot saved with SHA-256 hash, index={}, term={}", last_index, last_term);
  return Status::OK();
}

Status StatePersister::LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                                    uint64_t& last_term) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  if (!db_) {
    return Status::Error("StatePersister not open");
  }

  leveldb::Status s = db_->Get(leveldb::ReadOptions(), kSnapshotDataKey, &snapshot_data);
  if (s.IsNotFound()) {
    return Status::Error("No snapshot available");
  }
  if (!s.ok()) {
    return Status::Error("Failed to load snapshot: " + s.ToString());
  }

  std::string meta;
  s = db_->Get(leveldb::ReadOptions(), kSnapshotMetaKey, &meta);
  if (!s.ok() || meta.size() != 16) {
    return Status::Error("Failed to load snapshot metadata");
  }

  std::memcpy(&last_index, meta.data(), sizeof(last_index));
  std::memcpy(&last_term, meta.data() + 8, sizeof(last_term));

  std::string stored_hash;
  s = db_->Get(leveldb::ReadOptions(), kSnapshotHashKey, &stored_hash);
  if (s.ok() && stored_hash.size() == 32) {
    uint8_t computed_hash[32];
    ComputeSha256(snapshot_data.data(), snapshot_data.size(), computed_hash);
    if (std::memcmp(stored_hash.data(), computed_hash, 32) != 0) {
      LOG_ERROR("Snapshot SHA-256 mismatch! Data may be corrupted.");
      snapshot_data.clear();
      return Status::Error("Snapshot integrity check failed: SHA-256 mismatch");
    }
    LOG_DEBUG("Snapshot SHA-256 verified successfully");
  } else if (!s.IsNotFound()) {
    LOG_WARN("Could not load snapshot hash, skipping integrity check");
  } else {
    LOG_WARN("No SHA-256 hash found for snapshot, skipping integrity check");
  }

  return Status::OK();
}

Status StatePersister::SaveSnapshotStream(
    const std::function<bool(std::string& chunk)>& chunk_provider, uint64_t last_index,
    uint64_t last_term) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  if (!db_) {
    return Status::Error("StatePersister not open");
  }

  // Clean up any stale temp keys left by a previous interrupted attempt.
  DeleteSnapshotTempDataLocked();

  Sha256Context sha_ctx;
  Sha256Init(sha_ctx);

  uint32_t chunk_index = 0;
  std::string chunk;
  try {
    while (chunk_provider(chunk)) {
      if (!chunk.empty()) {
        std::string key = std::string(kSnapshotTmpChunkPrefix) + std::to_string(chunk_index);
        leveldb::Status s = db_->Put(leveldb::WriteOptions(), key, chunk);
        if (!s.ok()) {
          DeleteSnapshotTempDataLocked();
          return Status::Error("Failed to save snapshot chunk: " + s.ToString());
        }
        Sha256Update(sha_ctx, chunk.data(), chunk.size());
        ++chunk_index;
      }
    }
  } catch (const std::exception& e) {
    DeleteSnapshotTempDataLocked();
    return Status::Error("Snapshot chunk provider failed: " + std::string(e.what()));
  }

  if (chunk_index == 0) {
    // Empty snapshot: preserve legacy behavior of removing any existing
    // snapshot so that HasSnapshot() returns false.
    leveldb::WriteBatch batch;
    DeleteSnapshotDataLocked(&batch);
    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
      return Status::Error("Failed to clear old snapshot: " + s.ToString());
    }
    snapshot_last_index_ = 0;
    snapshot_last_term_ = 0;
    LOG_INFO("Snapshot save skipped: empty data");
    return Status::OK();
  }

  uint8_t hash[32];
  Sha256Final(sha_ctx, hash);

  char meta[20];
  std::memcpy(meta, &last_index, sizeof(last_index));
  std::memcpy(meta + 8, &last_term, sizeof(last_term));
  std::memcpy(meta + 16, &chunk_index, sizeof(chunk_index));

  // Build an atomic batch that deletes the old snapshot, copies the temp
  // chunks to their final keys, deletes the temp keys, and writes the new
  // metadata/hash. Either the whole batch commits or nothing does, so the
  // old snapshot remains intact if the swap fails.
  leveldb::WriteBatch batch;
  DeleteSnapshotDataLocked(&batch);

  batch.Put(kSnapshotMetaKey, leveldb::Slice(meta, sizeof(meta)));
  batch.Put(kSnapshotHashKey, leveldb::Slice(reinterpret_cast<const char*>(hash), 32));

  for (uint32_t i = 0; i < chunk_index; ++i) {
    std::string tmp_key = std::string(kSnapshotTmpChunkPrefix) + std::to_string(i);
    std::string final_key = std::string(kSnapshotChunkPrefix) + std::to_string(i);
    std::string chunk_data;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), tmp_key, &chunk_data);
    if (!s.ok()) {
      DeleteSnapshotTempDataLocked();
      return Status::Error("Failed to read temp snapshot chunk: " + s.ToString());
    }
    batch.Put(final_key, chunk_data);
    batch.Delete(tmp_key);
  }

  leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
  if (!s.ok()) {
    // The batch did not commit; old snapshot keys are unchanged. Clean up
    // temp keys so they do not accumulate.
    DeleteSnapshotTempDataLocked();
    return Status::Error("Failed to commit snapshot: " + s.ToString());
  }

  snapshot_last_index_ = last_index;
  snapshot_last_term_ = last_term;

  LOG_INFO("Snapshot saved (streaming, {} chunks), index={}, term={}", chunk_index, last_index,
           last_term);
  return Status::OK();
}

Status StatePersister::LoadSnapshotStream(
    const std::function<void(const std::string& chunk)>& chunk_consumer, uint64_t& last_index,
    uint64_t& last_term) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  if (!db_) {
    return Status::Error("StatePersister not open");
  }

  // Check old-format snapshot
  std::string old_value;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), kSnapshotDataKey, &old_value);
  if (s.ok()) {
    auto status = LoadSnapshot(old_value, last_index, last_term);
    if (status.ok() && !old_value.empty()) {
      chunk_consumer(old_value);
    }
    return status;
  }

  // New format
  std::string meta;
  s = db_->Get(leveldb::ReadOptions(), kSnapshotMetaKey, &meta);
  if (s.IsNotFound()) {
    return Status::Error("No snapshot available");
  }
  if (!s.ok() || (meta.size() != 16 && meta.size() != 20)) {
    return Status::Error("Failed to load snapshot metadata");
  }

  std::memcpy(&last_index, meta.data(), sizeof(last_index));
  std::memcpy(&last_term, meta.data() + 8, sizeof(last_term));

  uint32_t chunk_count = 0;
  if (meta.size() == 20) {
    std::memcpy(&chunk_count, meta.data() + 16, sizeof(chunk_count));
  }

  std::string stored_hash;
  s = db_->Get(leveldb::ReadOptions(), kSnapshotHashKey, &stored_hash);

  Sha256Context sha_ctx;
  Sha256Init(sha_ctx);
  uint32_t chunk_index = 0;
  std::string chunk;

  while (true) {
    if (meta.size() == 20 && chunk_index >= chunk_count) {
      break;
    }
    std::string key = std::string(kSnapshotChunkPrefix) + std::to_string(chunk_index);
    s = db_->Get(leveldb::ReadOptions(), key, &chunk);
    if (s.IsNotFound()) {
      break;
    }
    if (!s.ok()) {
      return Status::Error("Failed to load snapshot chunk: " + s.ToString());
    }
    Sha256Update(sha_ctx, chunk.data(), chunk.size());
    chunk_consumer(chunk);
    ++chunk_index;
  }

  uint8_t computed_hash[32];
  Sha256Final(sha_ctx, computed_hash);

  if (stored_hash.size() == 32) {
    if (std::memcmp(stored_hash.data(), computed_hash, 32) != 0) {
      LOG_ERROR("Snapshot SHA-256 mismatch! Data may be corrupted.");
      return Status::Error("Snapshot integrity check failed: SHA-256 mismatch");
    }
    LOG_DEBUG("Snapshot SHA-256 verified successfully");
  } else {
    LOG_WARN("No SHA-256 hash found for snapshot, skipping integrity check");
  }

  return Status::OK();
}

bool StatePersister::HasSnapshot() const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);

  if (!db_) {
    return false;
  }

  std::string value;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), kSnapshotDataKey, &value);
  if (s.ok()) {
    return true;
  }

  s = db_->Get(leveldb::ReadOptions(), kSnapshotMetaKey, &value);
  return s.ok();
}

uint64_t StatePersister::GetSnapshotLastIndex() const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  return snapshot_last_index_;
}

uint64_t StatePersister::GetSnapshotLastTerm() const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  return snapshot_last_term_;
}

void StatePersister::DeleteSnapshotDataLocked() {
  db_->Delete(leveldb::WriteOptions(), kSnapshotDataKey);
  db_->Delete(leveldb::WriteOptions(), kSnapshotMetaKey);
  db_->Delete(leveldb::WriteOptions(), kSnapshotHashKey);

  std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(leveldb::ReadOptions()));
  std::string prefix(kSnapshotChunkPrefix);
  for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
    db_->Delete(leveldb::WriteOptions(), it->key());
  }
}

void StatePersister::DeleteSnapshotDataLocked(leveldb::WriteBatch* batch) {
  batch->Delete(kSnapshotDataKey);
  batch->Delete(kSnapshotMetaKey);
  batch->Delete(kSnapshotHashKey);

  std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(leveldb::ReadOptions()));
  std::string prefix(kSnapshotChunkPrefix);
  for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
    batch->Delete(it->key());
  }
}

void StatePersister::DeleteSnapshotTempDataLocked() {
  std::unique_ptr<leveldb::Iterator> it(db_->NewIterator(leveldb::ReadOptions()));
  std::string prefix(kSnapshotTmpChunkPrefix);
  for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
    db_->Delete(leveldb::WriteOptions(), it->key());
  }
}

void StatePersister::LoadStateFromDB() {
  cached_state_ = PersistentState{};

  std::string term_value;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), kStateTermKey, &term_value);
  if (s.ok() && term_value.size() == sizeof(Term)) {
    std::memcpy(&cached_state_.current_term, term_value.data(), sizeof(cached_state_.current_term));
  }

  std::string voted_value;
  s = db_->Get(leveldb::ReadOptions(), kStateVotedForKey, &voted_value);
  if (s.ok() && voted_value.size() == sizeof(NodeId)) {
    std::memcpy(&cached_state_.voted_for, voted_value.data(), sizeof(cached_state_.voted_for));
  }
}

}  // namespace rollingraft
