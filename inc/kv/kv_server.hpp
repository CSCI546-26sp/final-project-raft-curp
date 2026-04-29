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

#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

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
    proposal_worker_ = std::thread([this]() {
        while (!stopped_.load()) {
            std::unique_lock<std::mutex> lk(proposal_mu_);
            proposal_cv_.wait(lk, [this] {
                return !proposal_queue_.empty() || stopped_.load();
            });
            while (!proposal_queue_.empty()) {
                std::string op = std::move(proposal_queue_.front());
                proposal_queue_.pop();
                lk.unlock();

                fprintf(stderr, "[CURP] background proposing: %s\n", op.c_str());

                auto proposal = raft_.propose(op);
                fprintf(stderr, "[CURP] propose returned: is_leader=%d index=%lu\n",
                    proposal.is_leader, proposal.index);

                if (proposal.is_leader) {
                    std::lock_guard<std::mutex> lock(mu_);
                    fast_path_indices_.insert(proposal.index);
                }

                lk.lock();
            }
        }
    });

    // recovery trigger: when this node becomes leader, freeze witness
    // and replay pending witness ops into Raft before serving new requests.
    raft_.set_on_become_leader(
        [this](uint64_t term) { this->on_become_leader(term); });
  }

  ~KvServer() {
    // TODO (lab 3): clean up any background threads.
    stopped_.store(true);
    proposal_cv_.notify_all();
    if (proposal_worker_.joinable()) {
        proposal_worker_.join();
    }
  }

  void enqueue_fast_path_proposal(const std::string& op) {
    {
      std::lock_guard<std::mutex> lk(proposal_mu_);
      proposal_queue_.push(op);
    }
    proposal_cv_.notify_one();
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
    fprintf(stderr, "[CURP] on_apply index=%lu valid=%d data=%s\n",
            result.index, result.valid, result.data.c_str());
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
          #ifdef TRACING
            auto tracer = tracing::get_tracer();
            auto bg_span = tracer->StartSpan("curp.background_committed");
            bg_span->SetAttribute("key", key);
            bg_span->SetAttribute("raft.index", result.index);
            bg_span->End();
          #endif
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
    if (raft_.has_unsynced_ops()) {
      raft_.witness_gc(result.index);
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

    #ifdef TRACING
      auto tracer = tracing::get_tracer();
      auto span = tracer->StartSpan("curp.put_fast_path");
      span->SetAttribute("key", request->key());
      auto scope = tracer->WithActiveSpan(span);
    #endif

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

    // Always reply immediately — master never waits for Raft
    // {
    //   std::lock_guard<std::mutex> lock(mu_);
    //   rifl_cache_[request->client_id()] = {
    //     request->seq_num(), {"", kvpb::KV_SUCCESS}
    //   };
    // }
    response->set_status(kvpb::KV_SUCCESS);
    enqueue_fast_path_proposal(op);

    #ifdef TRACING
      span->SetAttribute("curp.fast_path_reply", true);
      span->End();
    #endif

    return grpc::Status::OK;

    // auto proposal = raft_.propose(op);
    // if(!proposal.is_leader) {
    //   response->set_status(kvpb::KV_NOTLEADER);
    //   return grpc::Status::OK;
    // }

    // auto fut = register_waiter(proposal.index);

    // if(fut.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
    //   std::lock_guard<std::mutex> lock(mu_);
    //   waiters_.erase(proposal.index);
    //   abandoned_indices_.insert(proposal.index);
    //   response->set_status(kvpb::KV_TIMEOUT);
    //   return grpc::Status::OK;
    // }

    // auto op_result = fut.get();
    // response->set_status(op_result.status);
    // return grpc::Status::OK;
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

    // Always reply immediately — master never waits for Raft
    // {
    //   std::lock_guard<std::mutex> lock(mu_);
    //   rifl_cache_[request->client_id()] = {
    //     request->seq_num(), {"", kvpb::KV_SUCCESS}
    //   };
    // }
    response->set_status(kvpb::KV_SUCCESS);
    enqueue_fast_path_proposal(op);
    return grpc::Status::OK;

    // auto proposal = raft_.propose(op);
    // if(!proposal.is_leader) {
    //   response->set_status(kvpb::KV_NOTLEADER);
    //   return grpc::Status::OK;
    // }

    // auto fut = register_waiter(proposal.index);

    // if(fut.wait_for(std::chrono::seconds(5)) == std::future_status::timeout) {
    //   std::lock_guard<std::mutex> lock(mu_);
    //   waiters_.erase(proposal.index);
    //   abandoned_indices_.insert(proposal.index);
    //   response->set_status(kvpb::KV_TIMEOUT);
    //   return grpc::Status::OK;
    // }

    // auto op_result = fut.get();
    // response->set_status(op_result.status);
    // return grpc::Status::OK;
  }

  grpc::Status Sync(grpc::ServerContext *context,
                  const kvpb::SyncRequest *request,
                  kvpb::SyncResponse *response) override {
  (void)context;

  if (!raft_.get_state().is_leader) {
    response->set_status(kvpb::KV_NOTLEADER);
    return grpc::Status::OK;
  }

  #ifdef TRACING
    auto tracer = tracing::get_tracer();
    auto sync_span = tracer->StartSpan("curp.sync_wait");
    sync_span->SetAttribute("client_id", (int64_t)request->client_id());
    sync_span->SetAttribute("seq_num", (int64_t)request->seq_num());
    auto scope = tracer->WithActiveSpan(sync_span);
  #endif

  // Wait until this client's seq_num appears in rifl_cache_
  // meaning on_apply() has processed and committed it to store_
  const auto wait_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);

  while (true) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = rifl_cache_.find(request->client_id());
      if (it != rifl_cache_.end() && it->second.seq_num >= request->seq_num()) {
        // op has been committed and applied to store_
        #ifdef TRACING
          sync_span->SetAttribute("curp.sync_result", "committed");
          sync_span->End();
        #endif
        response->set_status(kvpb::KV_SUCCESS);
        return grpc::Status::OK;
      }
    }

    if (std::chrono::steady_clock::now() >= wait_deadline) {
      #ifdef TRACING
      sync_span->SetAttribute("curp.sync_result", "timeout");
      sync_span->End();
#endif
      response->set_status(kvpb::KV_TIMEOUT);
      return grpc::Status::OK;
    }

    if (!raft_.get_state().is_leader) {
      #ifdef TRACING
      sync_span->SetAttribute("curp.sync_result", "not_leader");
      sync_span->End();
#endif
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    // Wait for on_apply() to fire and update rifl_cache_
    std::unique_lock<std::mutex> lk(kv_applied_wait_mu_);
    kv_applied_cv_.wait_until(lk, wait_deadline, [&] {
      std::lock_guard<std::mutex> lock(mu_);
      auto it = rifl_cache_.find(request->client_id());
      return it != rifl_cache_.end() && it->second.seq_num >= request->seq_num();
    });
  }
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
    // Freeze witness for recovery: reject new WitnessRecord until explicitly ended.
    raft_.witness_enter_recovery();
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

  grpc::Status WitnessEndRecovery(grpc::ServerContext *context,
                                 const kvpb::WitnessEndRecoveryRequest *request,
                                 kvpb::WitnessEndRecoveryReply *response) override {
    (void)context;
    if (request->clear()) {
      raft_.witness_clear();
    }
    raft_.witness_exit_recovery();
    response->set_status(kvpb::KV_SUCCESS);
    return grpc::Status::OK;
  }

  void on_become_leader(uint64_t term) {
    // Quorum recovery: freeze a quorum of witnesses (WitnessGetRecoveryData),
    // replay recovered ops into Raft, then unfreeze/reset witnesses.
    auto parse_port = [](const std::string &addr) -> std::optional<uint64_t> {
      auto pos = addr.rfind(':');
      if (pos == std::string::npos || pos + 1 >= addr.size()) {
        return std::nullopt;
      }
      try {
        return static_cast<uint64_t>(std::stoull(addr.substr(pos + 1)));
      } catch (...) {
        return std::nullopt;
      }
    };
    auto host_part = [](const std::string &addr) -> std::string {
      auto pos = addr.rfind(':');
      if (pos == std::string::npos) return addr;
      return addr.substr(0, pos);
    };

    const auto raft_addr = raft_.get_listening_addr();
    const auto raft_port_opt = parse_port(raft_addr);
    if (!raft_port_opt.has_value()) {
      return;
    }
    const uint64_t kv_port = *raft_port_opt + 1000;
    const std::string self_kv_addr = host_part(raft_addr) + ":" + std::to_string(kv_port);

    std::vector<std::string> witness_kv_addrs;
    witness_kv_addrs.push_back(self_kv_addr);
    for (const auto &[pid, paddr] : raft_.get_peer_addrs()) {
      (void)pid;
      auto pport_opt = parse_port(paddr);
      if (!pport_opt.has_value()) continue;
      witness_kv_addrs.push_back(host_part(paddr) + ":" +
                                 std::to_string(*pport_opt + 1000));
    }

    const size_t n = witness_kv_addrs.size();
    const size_t quorum = (n / 2) + 1;

    struct RecoveryResp {
      std::string addr;
      bool ok{false};
      kvpb::WitnessGetRecoveryDataReply reply;
    };

    auto fetch_one = [](const std::string &addr) -> RecoveryResp {
      RecoveryResp out;
      out.addr = addr;
      auto channel =
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      auto stub = kvpb::KvService::NewStub(channel);
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::milliseconds(250));
      kvpb::WitnessGetRecoveryDataRequest req;
      kvpb::WitnessGetRecoveryDataReply rep;
      auto st = stub->WitnessGetRecoveryData(&ctx, req, &rep);
      out.ok = st.ok();
      out.reply = std::move(rep);
      return out;
    };

    std::vector<std::future<RecoveryResp>> futs;
    futs.reserve(witness_kv_addrs.size());
    for (const auto &addr : witness_kv_addrs) {
      futs.push_back(std::async(std::launch::async, fetch_one, addr));
    }

    std::vector<RecoveryResp> got;
    got.reserve(quorum);
    for (auto &f : futs) {
      auto r = f.get();
      if (r.ok) {
        got.push_back(std::move(r));
        if (got.size() >= quorum) break;
      }
    }

    // If we couldn't freeze a quorum, do nothing (next leader will retry).
    if (got.size() < quorum) {
      return;
    }

    // Pick a canonical recovery set: use the first successful witness's list.
    // (Matches CURP intuition: replay from one consistent witness table.)
    std::vector<kvpb::WitnessOp> to_replay;
    for (const auto &op : got.front().reply.ops()) {
      to_replay.push_back(op);
    }

    for (const auto &op : to_replay) {
      if (op.op_type() == "PUT" || op.op_type() == "APPEND") {
        std::string data = op.op_type() + "\t" + op.key() + "\t" + op.value() +
                           "\t" + std::to_string(op.client_id()) + "\t" +
                           std::to_string(op.seq_num());
        this->enqueue_fast_path_proposal(data);
      }
    }

    // Wait bounded for replayed ops to apply (RIFL dedups committed ops).
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (const auto &op : to_replay) {
      while (std::chrono::steady_clock::now() < deadline) {
        if (!raft_.get_state().is_leader || raft_.get_current_term() != term) {
          break;
        }
        {
          std::lock_guard<std::mutex> lock(mu_);
          auto it = rifl_cache_.find(op.client_id());
          if (it != rifl_cache_.end() && it->second.seq_num >= op.seq_num()) {
            break;
          }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
      }
    }

    auto end_one = [](const std::string &addr) {
      auto channel =
          grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
      auto stub = kvpb::KvService::NewStub(channel);
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::milliseconds(250));
      kvpb::WitnessEndRecoveryRequest req;
      req.set_clear(true);
      kvpb::WitnessEndRecoveryReply rep;
      (void)stub->WitnessEndRecovery(&ctx, req, &rep);
    };

    // Unfreeze/reset the witnesses we froze (best-effort).
    for (const auto &r : got) {
      end_one(r.addr);
    }
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
  std::mutex                   proposal_mu_;
  std::condition_variable      proposal_cv_;
  std::thread                  proposal_worker_;
  std::atomic<bool>            stopped_{false};

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
