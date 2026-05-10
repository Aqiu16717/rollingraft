/**
 * @file leveldb_persister.cpp
 * @brief LevelDB-based persistence implementation
 *
 * Implements the Persister interface using LevelDB as the
 * underlying storage engine. Provides durable storage for
 * Raft state, log entries, and snapshots.
 */

#include <cstring>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <shared_mutex>

#include "rollingraft/logger.h"
#include "rollingraft/persister.h"

namespace rollingraft {

// CRC32 lookup table (standard IEEE polynomial)
static const uint32_t kCrc32Table[256] = {
    0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
    0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,
    0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,
    0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,
    0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,
    0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
    0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,
    0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,
    0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,
    0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,
    0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
    0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,
    0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,
    0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,
    0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,
    0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
    0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,
    0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,
    0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,
    0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,
    0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
    0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,
    0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,
    0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,
    0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,
    0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
    0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,
    0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,
    0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,
    0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,
    0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
    0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,
    0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,
    0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,
    0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,
    0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
    0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,
    0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,
    0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,
    0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,
    0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
    0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,
    0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d
};

// Compute CRC32 checksum
static uint32_t ComputeCrc32(const char* data, size_t len) {
  uint32_t crc = 0xFFFFFFFF;
  for (size_t i = 0; i < len; ++i) {
    crc = (crc >> 8) ^ kCrc32Table[(crc ^ static_cast<uint8_t>(data[i])) & 0xFF];
  }
  return ~crc;
}

// ==================== SHA-256 Implementation ====================

// SHA-256 constants
static const uint32_t kSha256K[64] = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
    0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
    0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

// Right rotation
static inline uint32_t Sha256Ror(uint32_t x, uint32_t n) {
  return (x >> n) | (x << (32 - n));
}

// SHA-256 compression function helper
static inline void Sha256CompressRound(uint32_t& a, uint32_t& b, uint32_t& c,
                                       uint32_t& d, uint32_t& e, uint32_t& f,
                                       uint32_t& g, uint32_t& h, uint32_t k,
                                       uint32_t w) {
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

// Compute SHA-256 hash
// output must be 32 bytes
static void ComputeSha256(const char* data, size_t len, uint8_t* output) {
  // Initial hash values
  uint32_t h[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                   0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};

  // Process data in 64-byte chunks
  const size_t chunk_size = 64;
  size_t i = 0;

  while (i + chunk_size <= len) {
    uint32_t w[64];
    for (size_t j = 0; j < 16; ++j) {
      w[j] = (static_cast<uint8_t>(data[i + j * 4]) << 24) |
             (static_cast<uint8_t>(data[i + j * 4 + 1]) << 16) |
             (static_cast<uint8_t>(data[i + j * 4 + 2]) << 8) |
             static_cast<uint8_t>(data[i + j * 4 + 3]);
    }

    for (size_t j = 16; j < 64; ++j) {
      uint32_t s0 = Sha256Ror(w[j - 15], 7) ^ Sha256Ror(w[j - 15], 18) ^
                    (w[j - 15] >> 3);
      uint32_t s1 = Sha256Ror(w[j - 2], 17) ^ Sha256Ror(w[j - 2], 19) ^
                    (w[j - 2] >> 10);
      w[j] = w[j - 16] + s0 + w[j - 7] + s1;
    }

    uint32_t a = h[0], b = h[1], c = h[2], d = h[3];
    uint32_t e = h[4], f = h[5], g = h[6], hh = h[7];

    for (size_t j = 0; j < 64; ++j) {
      Sha256CompressRound(a, b, c, d, e, f, g, hh, kSha256K[j], w[j]);
    }

    h[0] += a;
    h[1] += b;
    h[2] += c;
    h[3] += d;
    h[4] += e;
    h[5] += f;
    h[6] += g;
    h[7] += hh;

    i += chunk_size;
  }

  // Handle final chunk with padding (simplified - assumes len < 2^55)
  uint8_t final_chunk[64] = {0};
  size_t remaining = len - i;
  std::memcpy(final_chunk, data + i, remaining);
  final_chunk[remaining] = 0x80;

  // Write length in bits as big-endian 64-bit integer
  uint64_t total_bits = len * 8;
  for (size_t j = 0; j < 8; ++j) {
    final_chunk[63 - j] = static_cast<uint8_t>(total_bits >> (j * 8));
  }

  // Process final chunk(s)
  for (size_t chunk = 0; chunk < 64; chunk += 64) {
    uint32_t w[64];
    for (size_t j = 0; j < 16; ++j) {
      w[j] = (final_chunk[chunk + j * 4] << 24) |
             (final_chunk[chunk + j * 4 + 1] << 16) |
             (final_chunk[chunk + j * 4 + 2] << 8) |
             final_chunk[chunk + j * 4 + 3];
    }

    for (size_t j = 16; j < 64; ++j) {
      uint32_t s0 = Sha256Ror(w[j - 15], 7) ^ Sha256Ror(w[j - 15], 18) ^
                    (w[j - 15] >> 3);
      uint32_t s1 = Sha256Ror(w[j - 2], 17) ^ Sha256Ror(w[j - 2], 19) ^
                    (w[j - 2] >> 10);
      w[j] = w[j - 16] + s0 + w[j - 7] + s1;
    }

    uint32_t a = h[0], b = h[1], c = h[2], d = h[3];
    uint32_t e = h[4], f = h[5], g = h[6], hh = h[7];

    for (size_t j = 0; j < 64; ++j) {
      Sha256CompressRound(a, b, c, d, e, f, g, hh, kSha256K[j], w[j]);
    }

    h[0] += a;
    h[1] += b;
    h[2] += c;
    h[3] += d;
    h[4] += e;
    h[5] += f;
    h[6] += g;
    h[7] += hh;
  }

  // Write output as big-endian
  for (size_t i = 0; i < 8; ++i) {
    output[i * 4] = static_cast<uint8_t>(h[i] >> 24);
    output[i * 4 + 1] = static_cast<uint8_t>(h[i] >> 16);
    output[i * 4 + 2] = static_cast<uint8_t>(h[i] >> 8);
    output[i * 4 + 3] = static_cast<uint8_t>(h[i]);
  }
}

// Key prefixes for different data types
constexpr char kStateKey[] = "state";
constexpr char kLogPrefix[] = "log:";
constexpr char kSnapshotKey[] = "snapshot";
constexpr char kSnapshotMetaKey[] = "snapshot_meta";
constexpr char kSnapshotHashKey[] = "snapshot_hash";  // SHA-256 checksum

class LevelDBPersister : public Persister {
 public:
  LevelDBPersister() = default;
  ~LevelDBPersister() override { Close(); }

  void SetSyncOnWrite(bool sync) override { sync_on_write_ = sync; }

  Status Open(const std::string& data_dir) override {
    std::unique_lock lock(mutex_);

    if (db_ != nullptr) {
      return Status::Error("Persister already open");
    }

    leveldb::Options options;
    options.create_if_missing = true;

    leveldb::DB* db_ptr = nullptr;
    leveldb::Status status = leveldb::DB::Open(options, data_dir, &db_ptr);

    if (!status.ok()) {
      return Status::Error("Failed to open LevelDB: " + status.ToString());
    }

    db_.reset(db_ptr);

    // Load cached state
    LoadStateFromDB();

    return Status::OK();
  }

  void Close() override {
    std::unique_lock lock(mutex_);
    db_.reset();
  }

  Status SaveState(const PersistentState& state) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    // Serialize: term (8 bytes) + voted_for (8 bytes)
    char buffer[16];
    std::memcpy(buffer, &state.current_term, sizeof(state.current_term));
    std::memcpy(buffer + 8, &state.voted_for, sizeof(state.voted_for));

    leveldb::Slice value(buffer, sizeof(buffer));
    leveldb::Status s = db_->Put(leveldb::WriteOptions(), kStateKey, value);

    if (!s.ok()) {
      return Status::Error("Failed to save state: " + s.ToString());
    }

    cached_state_ = state;
    return Status::OK();
  }

  Status LoadState(PersistentState& state) override {
    std::shared_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    state = cached_state_;
    return Status::OK();
  }

  Status AppendEntries(const std::vector<RaftLogEntry>& entries) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    if (entries.empty()) {
      return Status::OK();
    }

    leveldb::WriteBatch batch;

    for (const auto& entry : entries) {
      std::string key = MakeLogKey(entry.index_);
      std::string value = SerializeEntry(entry);
      batch.Put(key, value);
    }

    leveldb::WriteOptions write_options;
    write_options.sync = sync_on_write_;
    leveldb::Status s = db_->Write(write_options, &batch);
    if (!s.ok()) {
      return Status::Error("Failed to append entries: " + s.ToString());
    }

    return Status::OK();
  }

  Status GetEntries(uint64_t start, uint64_t end,
                    std::vector<RaftLogEntry>* out) override {
    std::shared_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    out->clear();

    if (start >= end) {
      return Status::OK();
    }

    // LevelDB is sorted, use iterator for range scan
    std::string start_key = MakeLogKey(start);
    std::string end_key = MakeLogKey(end);

    std::unique_ptr<leveldb::Iterator> it(
        db_->NewIterator(leveldb::ReadOptions()));
    for (it->Seek(start_key); it->Valid() && it->key().ToString() < end_key;
         it->Next()) {
      RaftLogEntry entry;
      if (DeserializeEntry(it->value(), entry)) {
        out->push_back(std::move(entry));
      }
    }

    if (!it->status().ok()) {
      return Status::Error("Failed to read entries: " +
                           it->status().ToString());
    }

    return Status::OK();
  }

  Status GetEntry(uint64_t index, RaftLogEntry& entry) override {
    std::shared_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    std::string key = MakeLogKey(index);
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &value);

    if (s.IsNotFound()) {
      return Status::Error("Entry not found");
    }
    if (!s.ok()) {
      return Status::Error("Failed to get entry: " + s.ToString());
    }

    if (!DeserializeEntry(value, entry)) {
      return Status::Error("Failed to deserialize entry");
    }

    return Status::OK();
  }

  Status TruncateSuffix(uint64_t from_index) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    // Get the last log index
    auto [last_index, _] = GetLastLogInfoLocked();

    if (from_index > last_index) {
      return Status::OK();
    }

    leveldb::WriteBatch batch;

    // Delete all entries in range [from_index, last_index]
    for (uint64_t i = from_index; i <= last_index; ++i) {
      batch.Delete(MakeLogKey(i));
    }

    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
      return Status::Error("Failed to truncate suffix: " + s.ToString());
    }

    return Status::OK();
  }

  Status TruncatePrefix(uint64_t before_index) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    if (before_index <= 1) {
      return Status::OK();
    }

    leveldb::WriteBatch batch;

    // Delete all entries in range [1, before_index)
    for (uint64_t i = 1; i < before_index; ++i) {
      batch.Delete(MakeLogKey(i));
    }

    leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) {
      return Status::Error("Failed to truncate prefix: " + s.ToString());
    }

    return Status::OK();
  }

  std::pair<uint64_t, uint64_t> GetLastLogInfo() override {
    std::shared_lock lock(mutex_);
    return GetLastLogInfoLocked();
  }

  Status SaveSnapshot(const std::string& snapshot_data, uint64_t last_index,
                      uint64_t last_term) override {
    std::unique_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    // Compute SHA-256 hash of snapshot data
    uint8_t hash[32];
    ComputeSha256(snapshot_data.data(), snapshot_data.size(), hash);

    leveldb::WriteBatch batch;

    // Save snapshot data
    batch.Put(kSnapshotKey, snapshot_data);

    // Save metadata: last_index (8 bytes) + last_term (8 bytes)
    char meta[16];
    std::memcpy(meta, &last_index, sizeof(last_index));
    std::memcpy(meta + 8, &last_term, sizeof(last_term));
    batch.Put(kSnapshotMetaKey, leveldb::Slice(meta, sizeof(meta)));

    // Save SHA-256 hash
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

  Status LoadSnapshot(std::string& snapshot_data, uint64_t& last_index,
                      uint64_t& last_term) override {
    std::shared_lock lock(mutex_);

    if (!db_) {
      return Status::Error("Persister not open");
    }

    // Load snapshot data
    leveldb::Status s =
        db_->Get(leveldb::ReadOptions(), kSnapshotKey, &snapshot_data);
    if (s.IsNotFound()) {
      return Status::Error("No snapshot available");
    }
    if (!s.ok()) {
      return Status::Error("Failed to load snapshot: " + s.ToString());
    }

    // Load metadata
    std::string meta;
    s = db_->Get(leveldb::ReadOptions(), kSnapshotMetaKey, &meta);
    if (!s.ok() || meta.size() != 16) {
      return Status::Error("Failed to load snapshot metadata");
    }

    std::memcpy(&last_index, meta.data(), sizeof(last_index));
    std::memcpy(&last_term, meta.data() + 8, sizeof(last_term));

    // Load and verify SHA-256 hash
    std::string stored_hash;
    s = db_->Get(leveldb::ReadOptions(), kSnapshotHashKey, &stored_hash);
    if (s.ok() && stored_hash.size() == 32) {
      // Compute hash of loaded data
      uint8_t computed_hash[32];
      ComputeSha256(snapshot_data.data(), snapshot_data.size(), computed_hash);

      // Compare hashes
      if (std::memcmp(stored_hash.data(), computed_hash, 32) != 0) {
        LOG_ERROR("Snapshot SHA-256 mismatch! Data may be corrupted.");
        snapshot_data.clear();
        return Status::Error("Snapshot integrity check failed: SHA-256 mismatch");
      }

      LOG_DEBUG("Snapshot SHA-256 verified successfully");
    } else if (!s.IsNotFound()) {
      // Hash exists but couldn't be read properly
      LOG_WARN("Could not load snapshot hash, skipping integrity check");
    } else {
      // No hash stored (backward compatibility)
      LOG_WARN("No SHA-256 hash found for snapshot, skipping integrity check");
    }

    return Status::OK();
  }

  bool HasSnapshot() const override {
    std::shared_lock lock(mutex_);
    if (!db_) return false;

    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), kSnapshotKey, &value);
    return s.ok();
  }

 private:
  void LoadStateFromDB() {
    std::string value;
    leveldb::Status s = db_->Get(leveldb::ReadOptions(), kStateKey, &value);

    if (s.ok() && value.size() == 16) {
      std::memcpy(&cached_state_.current_term, value.data(),
                  sizeof(cached_state_.current_term));
      std::memcpy(&cached_state_.voted_for, value.data() + 8,
                  sizeof(cached_state_.voted_for));
    }
    // If not found or error, use default values (0, -1)
  }

  std::pair<uint64_t, uint64_t> GetLastLogInfoLocked() {
    if (!db_) {
      return {0, 0};
    }

    // Find the last log entry
    std::unique_ptr<leveldb::Iterator> it(
        db_->NewIterator(leveldb::ReadOptions()));

    // Position at the end of log prefix range
    std::string prefix = kLogPrefix;
    std::string limit = prefix;
    limit.back()++;  // log: -> log;

    it->Seek(limit);
    if (it->Valid()) {
      it->Prev();
    } else {
      it->SeekToLast();
    }

    if (!it->Valid() || !it->key().starts_with(prefix)) {
      return {0, 0};
    }

    RaftLogEntry entry;
    if (DeserializeEntry(it->value(), entry)) {
      return {entry.index_, entry.term_};
    }

    return {0, 0};
  }

  std::string MakeLogKey(uint64_t index) {
    // Format: "log:{index:016x}" (16 hex digits for fixed width sorting)
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%s%016llx", kLogPrefix,
                  static_cast<unsigned long long>(index));
    return std::string(buf);
  }

  std::string SerializeEntry(const RaftLogEntry& entry) {
    // Format: index (4) + term (4) + data_len (4) + data + checksum (4)
    // Total: 16 + data_len bytes
    std::string result;
    result.reserve(16 + entry.data_.size());

    uint32_t term = entry.term_;
    uint32_t data_len = static_cast<uint32_t>(entry.data_.size());

    result.append(reinterpret_cast<const char*>(&entry.index_),
                  sizeof(entry.index_));
    result.append(reinterpret_cast<const char*>(&term), sizeof(term));
    result.append(reinterpret_cast<const char*>(&data_len), sizeof(data_len));
    result.append(entry.data_);

    // Calculate checksum over: index + term + data_len + data
    uint32_t checksum = ComputeCrc32(result.data(), result.size());
    result.append(reinterpret_cast<const char*>(&checksum), sizeof(checksum));

    return result;
  }

  bool DeserializeEntry(const leveldb::Slice& slice, RaftLogEntry& entry) {
    // Format: index (4) + term (4) + data_len (4) + data + checksum (4)
    // Minimum size: 16 bytes (with empty data)
    if (slice.size() < 16) {
      return false;
    }

    const char* data = slice.data();
    uint32_t term;
    uint32_t data_len;

    // Read header: index (4) + term (4) + data_len (4) = 12 bytes
    std::memcpy(&entry.index_, data, sizeof(entry.index_));
    std::memcpy(&term, data + 4, sizeof(term));
    std::memcpy(&data_len, data + 8, sizeof(data_len));

    // Expected size: header (12) + data + checksum (4) = 16 + data_len
    if (slice.size() != 16 + data_len) {
      return false;
    }

    // Verify checksum (over: index + term + data_len + data)
    uint32_t stored_checksum;
    std::memcpy(&stored_checksum, data + 12 + data_len, sizeof(stored_checksum));

    uint32_t computed_checksum = ComputeCrc32(data, 12 + data_len);
    if (computed_checksum != stored_checksum) {
      LOG_ERROR("CRC32 mismatch for log entry {}: expected={}, computed={}",
                entry.index_, stored_checksum, computed_checksum);
      return false;
    }

    entry.term_ = term;
    entry.data_.assign(data + 12, data_len);
    entry.checksum_ = stored_checksum;

    return true;
  }

 private:
  mutable std::shared_mutex mutex_;
  std::unique_ptr<leveldb::DB> db_;
  PersistentState cached_state_;

  // Cached snapshot info
  uint64_t snapshot_last_index_ = 0;
  uint64_t snapshot_last_term_ = 0;

  bool sync_on_write_ = false;
};

// Factory function implementation
std::unique_ptr<Persister> CreateLevelDBPersister() {
  return std::make_unique<LevelDBPersister>();
}

}  // namespace rollingraft
