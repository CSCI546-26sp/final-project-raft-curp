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
  std::thread([this]() {
    logger->info("Raft node {} run loop started", id);

    auto next_heartbeat = std::chrono::steady_clock::now() + heartbeat_interval;
    auto election_timeout = get_random_election_timeout();

    while (!this->is_dead()) {
      {
        std::unique_lock<std::mutex> lock(this->mtx);
        auto now = std::chrono::steady_clock::now();

        if (this->role == Role::Leader) {
          if (now >= next_heartbeat) {   // is this correct? or should we check the elapsed time since last heartbeat?
            next_heartbeat = now + heartbeat_interval;

            auto term = this->current_term;

            // send heartbeats w/o holding lock
            lock.unlock();
            this->send_heartbeats(term);
            lock.lock();
          }
        } else {
          auto elapsed = now - last_heartbeat;

          if (elapsed >= election_timeout) {
            // election timeout, start new election
            this->current_term += 1;
            this->role = Role::Candidate;
            this->voted_for = this->id;
            this->vote_count = 1;
            this->last_heartbeat = now;
            election_timeout = get_random_election_timeout();

            logger->info("Raft node {} starting election for term {}", id, current_term);

            auto term = this->current_term;

            // send RequestVote RPCs w/o holding lock
            lock.unlock();
            this->send_request_votes(term);
            lock.lock();
          }
        }
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    
    logger->info("Raft node {} run loop exit", id);
  }).detach();
}

State Raft::get_state() const {
  // TODO: lab 1
  std::lock_guard<std::mutex> lock(this->mtx);
  State s;
  s.term = this->current_term;
  s.is_leader = (this->role == Role::Leader);
  return s;
}

ProposalResult Raft::propose(const std::string &data) {
  // TODO: lab 2
  (void) data; // silencing unused warning for now

  std::lock_guard<std::mutex> lock(this->mtx);

  ProposalResult res;
  res.index = 0; // dummy index for now
  res.term = this->current_term; // whatever term we are on
  res.is_leader = (this->role == Role::Leader);
  return res;
}

ProposalResult Raft::propose_sync(const std::string &data) {
  // TODO: lab 3
  return propose(data);
}

// TODO: add more functions if desired.

std::chrono::milliseconds Raft::get_random_election_timeout() {
  auto min_ms = election_timeout_min.count();
  auto max_ms = election_timeout_max.count();

  if (max_ms <= min_ms) {
    return election_timeout_min;
  }

  auto range = static_cast<uint64_t>(max_ms - min_ms + 1);
  auto &gen = utils::RandGen::get_instance();
  auto offset = gen.intn(range);

  return std::chrono::milliseconds(min_ms + static_cast<uint64_t>(offset));
}

void Raft::send_heartbeats(uint64_t term) {
  for (auto &p: peers_) {
    auto target_id = p.first;
    auto &stub = p.second;

    if (!stub) {
      continue;
    }

    raftpb::AppendEntriesRequest req;
    req.set_term(term);
    req.set_leader_id(id);
    req.set_prev_log_index(0);
    req.set_prev_log_term(0);
    req.set_leader_commit(0);

    auto context = this->create_context(target_id);
    raftpb::AppendEntriesReply reply;

    grpc::Status status = stub->AppendEntries(context.get(), req, &reply);
    if (!status.ok()) {
      continue;
    }

    std::lock_guard<std::mutex> lock(this->mtx);

    if (reply.term() > this->current_term) {
      this->current_term = reply.term();
      this->role = Role::Follower;
      this->voted_for.reset();
      logger->info("Raft node {} stepping down", id);
    }
  }

  return;
}

void Raft::send_request_votes(uint64_t term) {
  return;
}

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
