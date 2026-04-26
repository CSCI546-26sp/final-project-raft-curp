#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "kv.grpc.pb.h"

namespace kv {

// ---------------------------------------------------------------------------
// KvClient is a fault-tolerant client for the KV service.
//
// It maintains stubs to all replicas and automatically retries operations
// when it encounters KV_NOTLEADER (rotating to the next replica) or
// transient gRPC failures.
//
// Each KvClient instance has a unique random client_id and a monotonically
// increasing seq_num, implementing the client side of RIFL.
//
// Usage:
//   std::vector<std::string> addrs = {"localhost:51050", "localhost:51051", "localhost:51052"};
//   kv::KvClient client(addrs);
//   client.put("key", "value");
//   auto [status, val] = client.get("key");
//   client.append("key", " more");
// ---------------------------------------------------------------------------
class KvClient {
public:
  explicit KvClient(const std::vector<std::string> &addrs) : leader_idx_(0) {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    client_id_ = gen();
    seq_num_ = 0;

    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 500);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 100);
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);

    for (const auto &addr : addrs) {
      auto channel = grpc::CreateCustomChannel(
          addr, grpc::InsecureChannelCredentials(), args);
      stubs_.push_back(kvpb::KvService::NewStub(std::move(channel)));
    }
  }

  kvpb::KvStatus put(const std::string &key, const std::string &value) {
    uint64_t seq = ++seq_num_;
    for (int attempt = 0; attempt < max_attempts_; ++attempt) {
      kvpb::PutRequest request;
      request.set_key(key);
      request.set_value(value);
      request.set_client_id(client_id_);
      request.set_seq_num(seq);

      kvpb::KvResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                           rpc_timeout_);

      auto status = stubs_[leader_idx_]->Put(&context, request, &response);
      if (status.ok() && response.status() == kvpb::KV_SUCCESS) {
        return kvpb::KV_SUCCESS;
      }
      // KV_NOTLEADER, KV_TIMEOUT, or gRPC failure — rotate to next node
      rotate_leader();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return kvpb::KV_TIMEOUT;
  }

  // CURP-style PUT: try witness superquorum first, then call the existing
  // leader RPC (slow path). If the leader returns early when witnesses
  // succeeded, this becomes the 1-RTT path.
  kvpb::KvStatus put_curp(const std::string &key, const std::string &value) {
    uint64_t seq = ++seq_num_;

    auto witness_fut = std::async(std::launch::async, [&]() {
      return witness_superquorum_record("PUT", key, value, client_id_, seq, 2);
    });

    for (int attempt = 0; attempt < max_attempts_; ++attempt) {
      kvpb::PutRequest request;
      request.set_key(key);
      request.set_value(value);
      request.set_client_id(client_id_);
      request.set_seq_num(seq);

      kvpb::KvResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);
      auto status = stubs_[leader_idx_]->Put(&context, request, &response);

      bool witnesses_ok = witness_fut.get();

      if (status.ok() && response.status() == kvpb::KV_SUCCESS) {
        if(witnesses_ok){
          return kvpb::KV_SUCCESS;
        }
        return sync_latest();
      }
      rotate_leader();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return kvpb::KV_TIMEOUT;
  }

  // Attempt CURP fast-path witness recording: returns true if a superquorum of witnesses replied conflict-free
  bool witness_superquorum_record(const std::string &op_type,
                                  const std::string &key,
                                  const std::string &value,
                                  uint64_t client_id, uint64_t seq_num,
                                  size_t required_ok = 4) {
    if (stubs_.empty()) {
      return false;
    }

    std::vector<std::future<std::optional<kvpb::WitnessRecordReply>>> futs;
    futs.reserve(stubs_.size());

    for (auto &stub : stubs_) {
      futs.emplace_back(std::async(std::launch::async, [&stub, &op_type, &key, &value, client_id, seq_num]() {
        kvpb::WitnessRecordRequest request;
        request.set_op_type(op_type);
        request.set_key(key);
        request.set_value(value);
        request.set_client_id(client_id);
        request.set_seq_num(seq_num);

        kvpb::WitnessRecordReply response;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(750));

        auto status = stub->WitnessRecord(&context, request, &response);
        if (!status.ok()) {
          return std::optional<kvpb::WitnessRecordReply>{};
        }
        return std::optional<kvpb::WitnessRecordReply>(response);
      }));
    }

    size_t ok = 0;
    for (auto &f : futs) {
      auto opt = f.get();
      if (!opt.has_value()) {
        continue;
      }
      if (opt->conflict()) {
        return false;
      }
      ++ok;
      if (ok >= required_ok) {
        return true;
      }
    }
    return false;
  }

  std::pair<kvpb::KvStatus, std::string> get(const std::string &key) {
    uint64_t seq = ++seq_num_;
    for (int attempt = 0; attempt < max_attempts_; ++attempt) {
      kvpb::GetRequest request;
      request.set_key(key);
      request.set_client_id(client_id_);
      request.set_seq_num(seq);

      kvpb::GetResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                           rpc_timeout_);

      auto status = stubs_[leader_idx_]->Get(&context, request, &response);
      if (status.ok() && response.status() == kvpb::KV_SUCCESS) {
        return {kvpb::KV_SUCCESS, response.value()};
      }
      // KV_NOTLEADER, KV_TIMEOUT, or gRPC failure — rotate to next node
      rotate_leader();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return {kvpb::KV_TIMEOUT, ""};
  }

  std::pair<kvpb::KvStatus, std::string> get_curp(const std::string &key) {
    // Check any witness for unsynced write on this key
    bool has_conflict = false;
    for (auto &stub : stubs_) {
      kvpb::WitnessRecordRequest request;
      request.set_op_type("GET");
      request.set_key(key);
      request.set_client_id(client_id_);
      request.set_seq_num(seq_num_); // don't increment — this is just a check

      kvpb::WitnessRecordReply response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                          std::chrono::milliseconds(750));

      auto status = stub->WitnessRecord(&context, request, &response);
      if (status.ok() && response.conflict()) {
        has_conflict = true;
        break;
      }
    }

    if (has_conflict) {
      // unsynced write on this key — wait for it to commit before reading
      auto s = sync_latest();
      if (s != kvpb::KV_SUCCESS) return {kvpb::KV_TIMEOUT, ""};
    }

    // safe to read — do normal get
    uint64_t seq = ++seq_num_;
    for (int attempt = 0; attempt < max_attempts_; ++attempt) {
      kvpb::GetRequest request;
      request.set_key(key);
      request.set_client_id(client_id_);
      request.set_seq_num(seq);

      kvpb::GetResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);

      auto status = stubs_[leader_idx_]->Get(&context, request, &response);
      if (status.ok() && response.status() == kvpb::KV_SUCCESS) {
        return {kvpb::KV_SUCCESS, response.value()};
      }
      rotate_leader();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return {kvpb::KV_TIMEOUT, ""};
  }

  kvpb::KvStatus append(const std::string &key, const std::string &value) {
    uint64_t seq = ++seq_num_;
    for (int attempt = 0; attempt < max_attempts_; ++attempt) {
      kvpb::AppendRequest request;
      request.set_key(key);
      request.set_value(value);
      request.set_client_id(client_id_);
      request.set_seq_num(seq);

      kvpb::KvResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() +
                           rpc_timeout_);

      auto status =
          stubs_[leader_idx_]->Append(&context, request, &response);
      if (status.ok() && response.status() == kvpb::KV_SUCCESS) {
        return kvpb::KV_SUCCESS;
      }
      // KV_NOTLEADER, KV_TIMEOUT, or gRPC failure — rotate to next node
      rotate_leader();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return kvpb::KV_TIMEOUT;
  }

  kvpb::KvStatus append_curp(const std::string &key, const std::string &value) {
    uint64_t seq = ++seq_num_;
    auto witness_fut = std::async(std::launch::async, [&]() {
      return witness_superquorum_record("APPEND", key, value, client_id_, seq, 2);
    });

    for (int attempt = 0; attempt < max_attempts_; ++attempt) {
      kvpb::AppendRequest request;
      request.set_key(key);
      request.set_value(value);
      request.set_client_id(client_id_);
      request.set_seq_num(seq);

      kvpb::KvResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);
      auto status = stubs_[leader_idx_]->Append(&context, request, &response);

      bool witnesses_ok = witness_fut.get();

      if (status.ok() && response.status() == kvpb::KV_SUCCESS) {
        if(witnesses_ok) {
          return kvpb::KV_SUCCESS;
        }
        return sync_latest();
      }
      rotate_leader();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return kvpb::KV_TIMEOUT;
  }

  // UNUSED -- added sync_latest instead
  kvpb::KvStatus sync(uint64_t client_id, uint64_t seq_num) {
    for (int attempt = 0; attempt < max_attempts_; ++attempt) {
      kvpb::SyncRequest request;
      request.set_client_id(client_id);
      request.set_seq_num(seq_num);

      kvpb::SyncResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);

      auto status = stubs_[leader_idx_]->Sync(&context, request, &response);
      if (status.ok() && response.status() == kvpb::KV_SUCCESS) {
        return kvpb::KV_SUCCESS;
      }
      rotate_leader();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return kvpb::KV_TIMEOUT;
  }

  kvpb::KvStatus sync_latest() {
    for (int attempt = 0; attempt < max_attempts_; ++attempt) {
      kvpb::GetRequest request;
      request.set_key("__sync__");
      request.set_client_id(client_id_);
      request.set_seq_num(++seq_num_);

      kvpb::GetResponse response;
      grpc::ClientContext context;
      context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);

      auto status = stubs_[leader_idx_]->Get(&context, request, &response);
      if (status.ok()) return kvpb::KV_SUCCESS;
      rotate_leader();
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return kvpb::KV_TIMEOUT;
  }

  uint64_t client_id() const { return client_id_; }

private:
  void rotate_leader() {
    leader_idx_ = (leader_idx_ + 1) % stubs_.size();
  }

  std::vector<std::unique_ptr<kvpb::KvService::Stub>> stubs_;
  uint64_t client_id_;
  std::atomic<uint64_t> seq_num_;
  size_t leader_idx_;

  static constexpr int max_attempts_ = 50;
  static constexpr std::chrono::seconds rpc_timeout_{5};
};

} // namespace kv
