#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
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
#include <queue>

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
    if (it != rifl_cache_.end() && it->second.seq_num == seq_num) {
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
      auto wit = waiters_.find(result.index);
      if(wit != waiters_.end()) {
        wit->second.set_value({"", kvpb::KV_TIMEOUT});
        waiters_.erase(wit);
      } else {
        // If an RPC timed out waiting for this index, don't keep a result
        // buffer around forever. The KV state is still correct (no-op for
        // invalid apply), and client retries will be handled via Raft/RIFL.
        if (abandoned_indices_.erase(result.index) == 0) {
          applied_results_[result.index] = {"", kvpb::KV_TIMEOUT};
        }
      }
      last_kv_applied_up_to_.store(result.index, std::memory_order_release);
      kv_applied_cv_.notify_all();
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

    OpResult op_result;
    {
      std::lock_guard<std::mutex> lock(mu_);

      auto it = rifl_cache_.find(client_id);
      if (it != rifl_cache_.end() && it->second.seq_num >= seq_num) {
        op_result = it->second.result;
      }
      else {
        if (op == "NOOP") {
          op_result = {"", kvpb::KV_SUCCESS};
        } else if (op == "PUT") {
          store_[key] = value;
          op_result = {"", kvpb::KV_SUCCESS};
          rifl_cache_[client_id] = {seq_num, op_result};
        } else if (op == "APPEND") {
          store_[key] += value;
          op_result = {"", kvpb::KV_SUCCESS};
          rifl_cache_[client_id] = {seq_num, op_result};
        } else if (op == "GET") {
          auto kit = store_.find(key);
          std::string val = (kit != store_.end()) ? kit->second : "";
          op_result = {val, kvpb::KV_SUCCESS};
          rifl_cache_[client_id] = {seq_num, op_result};
        }
      }

      auto wit = waiters_.find(result.index);
      if(wit != waiters_.end()) {
        wit->second.set_value(op_result);
        waiters_.erase(wit);
      } else if (fast_path_indices_.erase(result.index)) {
        // SKIP DO NOTHING
      } else {
        // If the waiting RPC already timed out, don't retain a buffered result.
        // We still applied to store_ and updated rifl_cache_ above.
        if (abandoned_indices_.erase(result.index) == 0) {
          applied_results_[result.index] = op_result;
        }
      }
      last_kv_applied_up_to_.store(result.index, std::memory_order_release);
      kv_applied_cv_.notify_all();
    }
    raft_.witness_gc(result.index);
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

    if (!raft_.get_state().is_leader) {
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

    if (!raft_.has_valid_lease()) {
      if (!raft_.read_quorum_barrier(std::chrono::milliseconds(300))) {
        response->set_status(kvpb::KV_NOTLEADER);
        return grpc::Status::OK;
      }
    }

    const uint64_t commit_upto = raft_.get_commit_index();

    const auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (last_kv_applied_up_to_.load(std::memory_order_acquire) < commit_upto) {
      if (std::chrono::steady_clock::now() >= wait_deadline) {
        response->set_status(kvpb::KV_TIMEOUT);
        return grpc::Status::OK;
      }
      if (!raft_.get_state().is_leader) {
        response->set_status(kvpb::KV_NOTLEADER);
        return grpc::Status::OK;
      }

      // Avoid calling into Raft from the CV predicate (extra locking per wakeup).
      std::unique_lock<std::mutex> lk(kv_applied_wait_mu_);
      (void)kv_applied_cv_.wait_until(lk, wait_deadline, [&] {
        return last_kv_applied_up_to_.load(std::memory_order_acquire) >= commit_upto;
      });
    }

    std::lock_guard<std::mutex> lock(mu_);
    auto kit = store_.find(request->key());
    std::string val = (kit != store_.end()) ? kit->second : "";
    OpResult op_result = {val, kvpb::KV_SUCCESS};
    rifl_cache_[request->client_id()] = {request->seq_num(), op_result};
    response->set_status(op_result.status);
    response->set_value(val);
    return grpc::Status::OK;
  }

  grpc::Status Append(grpc::ServerContext *context,
                      const kvpb::AppendRequest *request,
                      kvpb::KvResponse *response) override {
    // TODO (lab 3): implement
    (void)context;

    if (!raft_.get_state().is_leader) {
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

  grpc::Status WitnessRecord(grpc::ServerContext *context,
                             const kvpb::WitnessRecordRequest *request,
                             kvpb::WitnessRecordReply *response) override {
    (void)context;
    auto res = raft_.witness_record(request->op_type(), request->key(),
                                    request->value(), request->client_id(),
                                    request->seq_num());
    response->set_conflict(res.conflict);
    response->set_witness_idx(res.witness_idx);
    return grpc::Status::OK;
  }

  grpc::Status
  WitnessGetRecoveryData(grpc::ServerContext *context,
                         const kvpb::WitnessGetRecoveryDataRequest *request,
                         kvpb::WitnessGetRecoveryDataReply *response) override {
    (void)context;
    (void)request;
    auto ops = raft_.witness_get_recovery_data();
    for (const auto &op : ops) {
      auto *o = response->add_ops();
      o->set_op_type(op.op_type);
      o->set_key(op.key);
      o->set_value(op.value);
      o->set_client_id(op.client_id);
      o->set_seq_num(op.seq_num);
    }
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

  // Max Raft log index applied to store_/rifl (used to sync ReadIndex-style GETs).
  std::atomic<uint64_t> last_kv_applied_up_to_{0};
  std::mutex kv_applied_wait_mu_;
  std::condition_variable kv_applied_cv_;

  std::unordered_set<uint64_t> fast_path_indices_;
  std::queue<std::string>      proposal_queue_;

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
