#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <string>
#include <unordered_map>
#include <chrono>
#include <optional>
#include <thread>
#include <vector>
#include <atomic> 

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/msg_queue.hpp"

// it will pick up correct header
// when you generate the grpc proto files
#include "raft.grpc.pb.h"

using namespace toolings;

namespace rafty {
using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
using grpc::Server;

class Raft {
public:
  Raft(const Config &config, MessageQueue<ApplyResult> &ready);
  ~Raft();

  // WARN: do not modify the signature
  // TODO: implement `run`, `propose` and `get_state`
  void run(); /* lab 1 */
  ProposalResult propose(const std::string &data); /* lab 1 */
  State get_state() const; /* lab 2 */

  // lab3: sync propose
  ProposalResult propose_sync(const std::string &data);

  // Leader ReadIndex barrier: quorum-confirmed AppendEntries in current term.
  bool read_quorum_barrier(
      std::chrono::milliseconds timeout = std::chrono::milliseconds(500));
  uint64_t get_commit_index() const;
  // Avoid quorum
  bool has_valid_lease() const;

  // WARN: do not modify the signature
  void start_server();
  void stop_server();
  void connect_peers();
  bool is_dead() const;
  void kill();

  struct WitnessRecordResult {
    bool conflict;
    uint64_t witness_idx;
  };

  WitnessRecordResult witness_record(const std::string& op_type,
                               const std::string& key,
                               const std::string& value,
                               uint64_t client_id,
                               uint64_t seq_num);

  void witness_gc(uint64_t up_to);

  // register a callback invoked when this node becomes leader
  // the callback must be non-blocking & will run on the votehandling thread right after leader transition
  void set_on_become_leader(std::function<void(uint64_t)> cb);
  uint64_t get_current_term() const;
  uint64_t get_id() const;
  
private:
  // WARN: do not modify `create_context` and `apply`.

  // invoke `create_context` when creating context for rpc call.
  // args: the id of which raft instance the RPC will go to.
  std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;
  void apply(const ApplyResult &result);

  class RaftServiceImpl final : public raftpb::RaftService::Service {
    public:
      explicit RaftServiceImpl(Raft *raft) : raft_(raft) {}
      grpc::Status RequestVote(grpc::ServerContext *context,
                              const raftpb::RequestVoteRequest *request,
                              raftpb::RequestVoteReply *reply) override;
      grpc::Status AppendEntries(
          grpc::ServerContext *context, const raftpb::AppendEntriesRequest *request,
          raftpb::AppendEntriesReply *reply) override;

    private:
      Raft *raft_;
  };

protected:
  // WARN: do not modify `mtx` and `logger`.
  mutable std::mutex mtx;
  std::unique_ptr<rafty::utils::logger> logger;

private:
  // WARN: do not modify the declaration of
  // `id`, `listening_addr`, `peer_addrs`,
  // `dead`, `ready_queue`, `peers_`, and `server_`.
  uint64_t id;
  std::string listening_addr;
  std::map<uint64_t, std::string> peer_addrs;

  std::atomic<bool> dead;
  MessageQueue<ApplyResult> &ready_queue;

  std::unordered_map<uint64_t, RaftServiceStub> peers_;
  std::unique_ptr<Server> server_;

  std::unique_ptr<RaftServiceImpl> grpcService; // grpc server instance

  std::chrono::steady_clock::time_point last_quorum_ack_{};
  std::function<void(uint64_t)> on_become_leader_{};

  // Raft states
  enum class Role {
    Follower,
    Candidate,
    Leader
  };

  Role role{Role::Follower};

  uint64_t current_term{0};
  std::optional<uint64_t> voted_for;

  uint64_t vote_count{0};
  
  std::chrono::steady_clock::time_point last_heartbeat;

  std::chrono::milliseconds heartbeat_interval{50};
  std::chrono::milliseconds election_timeout_min{150};
  std::chrono::milliseconds election_timeout_max{300};

  std::vector<raftpb::Entry> log_entries; // for log entries (index 0 is dummy entry, real entries start from index 1)

  uint64_t commit_index{0};
  uint64_t last_applied{0};

  std::unordered_map<uint64_t, uint64_t> next_index; // for each server, the next log index to send
  std::unordered_map<uint64_t, uint64_t> match_index; // for each server, the highest log index known to be replicated

  std::chrono::milliseconds get_random_election_timeout();

  void send_heartbeats(uint64_t term);
  void send_heartbeats(uint64_t term, uint64_t min_commit_index);
  void send_request_votes(uint64_t term);

  // One AppendEntries RPC to a follower and full reply handling (replication +
  // commit + apply). Returns true iff RPC succeeded and reply.success().
  bool replicate_to_follower(uint64_t target_id, uint64_t term);

  // Background replication (one worker per follower).
  void start_replication_workers();
  void replication_worker(uint64_t target_id);
  void signal_replication();

  std::atomic<bool> repl_started_{false};
  std::mutex repl_mu_;
  std::condition_variable repl_cv_;
  std::atomic<uint64_t> repl_epoch_{0};
  std::vector<std::thread> repl_workers_;

  // CURP: unsynced witness ops
  struct UnsyncedOp {
    std::string key;
    std::string value;
    std::string op_type;
    uint64_t client_id;
    uint64_t seq_num;
  };

  // map from log index -> op (fast path ACK which are not yet in Raft)
  std::unordered_map <uint64_t, UnsyncedOp> unsynced_ops_;
  uint64_t next_unsynced_index_{0};
  mutable std::mutex witness_mtx_;
  bool witness_recovery_mode_{false};

public:
  std::vector<UnsyncedOp> witness_get_recovery_data();
  bool witness_has_unsynced_write(const std::string& key);
  bool has_unsynced_ops() const;
  void witness_enter_recovery();
  void witness_exit_recovery();
  bool witness_in_recovery() const;
};
} // namespace rafty

#include "rafty/impl/raft.ipp" // IWYU pragma: keep
