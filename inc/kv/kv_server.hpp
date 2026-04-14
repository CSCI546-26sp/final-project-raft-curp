#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "kv.grpc.pb.h"
#include "rafty/raft.hpp"
#include <future>
#include <sstream>
#include <chrono>
#include <optional>
#include <unordered_set>

namespace kv {

struct OpResult{
  std::string value;
  kvpb::KvStatus status;
};

struct RiflEntry{
  uint64_t seq_num;
  OpResult result;
};

class KvServer : public kvpb::KvService::Service {
public:
  explicit KvServer(rafty::Raft &raft) : raft_(raft) {
    // TODO (lab 3): initialize your data structures here.
    // You may want to start background threads, set up condition variables, etc.
  }

  ~KvServer() {
    // TODO (lab 3): clean up any background threads.
  }

  std::future<OpResult> register_waiter(uint64_t index){
    std::lock_guard<std::mutex> lock(mu_);

    // If already applied before waiter registration, return immediately.
    auto rit = applied_results_.find(index);
    if (rit != applied_results_.end()) {
      std::promise<OpResult> p;
      auto fut = p.get_future();
      p.set_value(rit->second);
      applied_results_.erase(rit);
      return fut;
    }

    auto &promise = waiters_[index];
    return promise.get_future();
  }

  std::optional<OpResult> check_rifl_cache(uint64_t client_id, uint64_t seq_num) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = rifl_cache_.find(client_id);
    if (it != rifl_cache_.end() && it->second.seq_num >= seq_num) {
      return it->second.result;
    }
    return std::nullopt;
  }

  // -----------------------------------------------------------------
  // on_apply is called by the node wrapper each time Raft commits a
  // log entry. The ApplyResult contains the index and data of the
  // committed entry (the same string you passed to raft_.propose()).
  //
  // You should:
  //   1. Deserialize the operation from result.data
  //   2. Apply it to your in-memory key/value map
  //   3. Handle RIFL duplicate detection
  //   4. Notify the waiting RPC handler that its operation has committed
  // -----------------------------------------------------------------
  void on_apply(const rafty::ApplyResult &result) {
    // TODO (lab 3): implement this.
    if(!result.valid) {
      std::lock_guard<std::mutex> lock(mu_);
      if (abandoned_indices_.erase(result.index) > 0) {
        return;
      }
      auto wit = waiters_.find(result.index);
      if(wit != waiters_.end()) {
        wit->second.set_value({"", kvpb::KV_TIMEOUT});
        waiters_.erase(wit);
      } else {
        applied_results_[result.index] = {"", kvpb::KV_TIMEOUT};
      }
      return;
    }
    std::istringstream ss(result.data);
    std::string op, key, value;
    uint64_t client_id, seq_num;
    std::getline(ss, op, '\t');
    std::getline(ss, key, '\t');
    std::getline(ss, value, '\t');
    ss >> client_id;
    ss.ignore();
    ss >> seq_num;

    std::lock_guard<std::mutex> lock(mu_);

    if (abandoned_indices_.erase(result.index) > 0) {
      return;
    }

    OpResult op_result;

    auto it = rifl_cache_.find(client_id);
    if (it != rifl_cache_.end() && it->second.seq_num >= seq_num) {
      op_result = it->second.result;
    }
    else {
      if (op == "PUT") {
        store_[key] = value;
        op_result = {"", kvpb::KV_SUCCESS};
      }
      else if (op == "APPEND") {
        store_[key] += value;
        op_result = {"", kvpb::KV_SUCCESS};
      }
      else if (op == "GET") {
        auto kit = store_.find(key);
        std::string val = (kit != store_.end()) ? kit->second : "";
        op_result = {val, kvpb::KV_SUCCESS};
      }

      rifl_cache_[client_id] = {seq_num, op_result};
    }

    auto wit = waiters_.find(result.index);
      if(wit != waiters_.end()) {
        wit->second.set_value(op_result);
        waiters_.erase(wit);
      } else {
        applied_results_[result.index] = op_result;
      }
  }

  // -----------------------------------------------------------------
  // gRPC handlers for client operations.
  //
  // Each handler should:
  //   1. Check if this node is the Raft leader (if not, return KV_NOTLEADER)
  //   2. Serialize the operation into a string
  //   3. Call raft_.propose(serialized_op) to submit to Raft
  //   4. Wait for on_apply() to process the committed entry at the
  //      returned index
  //   5. Return the result to the client
  //
  // Use RIFL (client_id + seq_num) to detect and handle duplicate
  // requests, just like Lab 0b.
  // -----------------------------------------------------------------

  grpc::Status Put(grpc::ServerContext *context,
                   const kvpb::PutRequest *request,
                   kvpb::KvResponse *response) override {
    // TODO (lab 3): implement
    (void)context;

    if(!raft_.get_state().is_leader) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    // RIFL duplicate detection: if already executed, return cached result.
    if (auto cached = check_rifl_cache(request->client_id(), request->seq_num())) {
      response->set_status(cached->status);
      return grpc::Status::OK;
    }

    std::string op = "PUT\t" + request->key() + "\t" + request->value() +
                      "\t" + std::to_string(request->client_id()) +
                      "\t" + std::to_string(request->seq_num());

    auto proposal = raft_.propose(op);
    if(!proposal.is_leader) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    auto fut = register_waiter(proposal.index);

    if(fut.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
      std::lock_guard<std::mutex> lock(mu_);
      waiters_.erase(proposal.index);
      abandoned_indices_.insert(proposal.index);
      response->set_status(kvpb::KV_TIMEOUT);
      return grpc::Status::OK;
    }

    auto op_result = fut.get();
    response->set_status(op_result.status);
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext *context,
                   const kvpb::GetRequest *request,
                   kvpb::GetResponse *response) override {
    // TODO (lab 3): implement
    (void)context;

    if(!raft_.get_state().is_leader) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    // RIFL duplicate detection: if already executed, return cached result.
    if (auto cached = check_rifl_cache(request->client_id(), request->seq_num())) {
      response->set_status(cached->status);
      response->set_value(cached->value);
      return grpc::Status::OK;
    }

    std::string op = "GET\t" + request->key() + "\t\t" +
                   std::to_string(request->client_id()) +
                   "\t" + std::to_string(request->seq_num());

    auto proposal = raft_.propose(op);
    if(!proposal.is_leader) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    auto fut = register_waiter(proposal.index);

    if(fut.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
      std::lock_guard<std::mutex> lock(mu_);
      waiters_.erase(proposal.index);
      abandoned_indices_.insert(proposal.index);
      response->set_status(kvpb::KV_TIMEOUT);
      return grpc::Status::OK;
    }

    auto op_result = fut.get();
    response->set_status(op_result.status);
    response->set_value(op_result.value);
    return grpc::Status::OK;
  }

  grpc::Status Append(grpc::ServerContext *context,
                      const kvpb::AppendRequest *request,
                      kvpb::KvResponse *response) override {
    // TODO (lab 3): implement
    (void)context;

    if(!raft_.get_state().is_leader) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    // RIFL duplicate detection: if already executed, return cached result.
    if (auto cached = check_rifl_cache(request->client_id(), request->seq_num())) {
      response->set_status(cached->status);
      return grpc::Status::OK;
    }

    std::string op = "APPEND\t" + request->key() + "\t" + request->value() +
                      "\t" + std::to_string(request->client_id()) +
                      "\t" + std::to_string(request->seq_num());

    auto proposal = raft_.propose(op);
    if(!proposal.is_leader) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    auto fut = register_waiter(proposal.index);

    if(fut.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
      std::lock_guard<std::mutex> lock(mu_);
      waiters_.erase(proposal.index);
      abandoned_indices_.insert(proposal.index);
      response->set_status(kvpb::KV_TIMEOUT);
      return grpc::Status::OK;
    }

    auto op_result = fut.get();
    response->set_status(op_result.status);
    return grpc::Status::OK;
  }

private:
  rafty::Raft &raft_;

  std::mutex mu_;

  std::unordered_map<std::string, std::string> store_;
  std::unordered_map<uint64_t, RiflEntry> rifl_cache_;
  std::unordered_map<uint64_t, std::promise<OpResult>> waiters_;
  std::unordered_map<uint64_t, OpResult> applied_results_;
  std::unordered_set<uint64_t> abandoned_indices_;

  // TODO (lab 3): add your state here. Consider:
  //
  // - std::unordered_map<std::string, std::string> store_;
  //       The in-memory key/value map.
  //
  // - RIFL tables: track (client_id -> highest seq_num) and cache the
  //   response for the last operation per client, so that duplicate
  //   requests return the cached result instead of re-executing.
  //
  // - A notification mechanism (e.g., std::condition_variable or
  //   std::promise/std::future per pending request) so that RPC handler
  //   threads can wait for their specific log entry to be committed
  //   and applied via on_apply().
  //
  // - std::mutex for protecting shared state.
};

} // namespace kv
