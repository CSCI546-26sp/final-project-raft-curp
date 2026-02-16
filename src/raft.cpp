#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
    : logger(utils::logger::get_logger(config.id)), id(config.id), listening_addr(config.addr),
      peer_addrs(config.peer_addrs), dead(false), ready_queue(ready), grpcService(std::make_unique<RaftServiceImpl>(this))
  // TODO: add more field if desired
{
  // TODO: finish it
  this->role = Role::Follower;
  this->current_term = 0;
  this->voted_for.reset();
  this->vote_count = 0;
  this->last_heartbeat = std::chrono::steady_clock::now();
  this->heartbeat_interval = std::chrono::milliseconds(100);
  this->election_timeout_min = std::chrono::milliseconds(300);
  this->election_timeout_max = std::chrono::milliseconds(500);
}

Raft::~Raft() { this->stop_server(); }

void Raft::run() {
  // TODO: kick off the raft instance
  // Note: this function should be non-blocking

  // lab 1
  
}

State Raft::get_state() const {
  // TODO: lab 1
}

ProposalResult Raft::propose(const std::string &data) {
  // TODO: lab 2
}

ProposalResult Raft::propose_sync(const std::string &data) {
  // TODO: lab 3
}

// TODO: add more functions if desired.

grpc::Status Raft::RaftServiceImpl::AppendEntries(
        grpc::ServerContext *context,
        const raftpb::AppendEntriesRequest *request,
        raftpb::AppendEntriesReply *reply){
    return grpc::Status::OK;
}

grpc::Status Raft::RaftServiceImpl::RequestVote(grpc::ServerContext *context,
                        const raftpb::RequestVoteRequest *request,
                        raftpb::RequestVoteReply *reply){
    return grpc::Status::OK;
}

} // namespace rafty
