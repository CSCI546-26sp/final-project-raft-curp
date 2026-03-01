#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <chrono>
#include <optional>

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

  // WARN: do not modify the signature
  void start_server();
  void stop_server();
  void connect_peers();
  bool is_dead() const;
  void kill();

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
  void send_request_votes(uint64_t term);

};
} // namespace rafty

#include "rafty/impl/raft.ipp" // IWYU pragma: keep
