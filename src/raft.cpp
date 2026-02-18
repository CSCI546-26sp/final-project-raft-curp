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
  this->heartbeat_interval = std::chrono::milliseconds(50);
  this->election_timeout_min = std::chrono::milliseconds(150);
  this->election_timeout_max = std::chrono::milliseconds(300);
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
  for (auto &p : peers_) {
    uint64_t target_id = p.first;
    auto *stub = p.second.get();
    if (!stub) {
      continue;
    }

    std::thread([this, term, target_id, stub]() {
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
        return;
      }

      std::lock_guard<std::mutex> lock(this->mtx);
      if (reply.term() > this->current_term) {
        this->current_term = reply.term();
        this->role = Role::Follower;
        this->voted_for.reset();
        this->last_heartbeat = std::chrono::steady_clock::now();
        logger->info("Raft node {} stepping down", id);
      }
    }).detach();
  }
}

void Raft::send_request_votes(uint64_t term) {
  size_t total_nodes = peer_addrs.size() + 1;
  size_t majority = total_nodes / 2 + 1;

  for (auto &p : peers_) {
    uint64_t target_id = p.first;
    auto *stub = p.second.get();
    if (!stub) {
      continue;
    }

    std::thread([this, term, target_id, majority]() {
      auto& stub = this->peers_[target_id];
      raftpb::RequestVoteRequest req;
      req.set_term(term);
      req.set_candidate_id(id);
      req.set_last_log_index(0);
      req.set_last_log_term(0);

      auto context = this->create_context(target_id);
      raftpb::RequestVoteReply reply;

      grpc::Status status = stub->RequestVote(context.get(), req, &reply);
      if (!status.ok()) {
        return;
      }

      std::lock_guard<std::mutex> lock(this->mtx);
      if (reply.term() > this->current_term) {
        this->current_term = reply.term();
        this->role = Role::Follower;
        this->voted_for.reset();
        this->vote_count = 0;
        this->last_heartbeat = std::chrono::steady_clock::now();
        return;
      }
      if (this->role == Role::Candidate && this->current_term == term &&
          reply.vote_granted()) {
        this->vote_count += 1;
        if (this->vote_count >= majority) {
          this->role = Role::Leader;
          logger->info("Raft node {} becomes leader for term {}", id,
                       this->current_term);
        }
      }
    }).detach();
  }
}

grpc::Status Raft::RaftServiceImpl::AppendEntries(
      grpc::ServerContext *context,
      const raftpb::AppendEntriesRequest *request,
      raftpb::AppendEntriesReply *reply){

    (void)context;
    std::lock_guard<std::mutex> lock(raft_->mtx);

    //can be stale entry
    if(request->term() < raft_->current_term) {
        reply->set_term(raft_->current_term);
        reply->set_success(false);
        return grpc::Status::OK;
    }
    // heartbeat and need to update ourself
    else if(request->term() > raft_->current_term) {
        raft_->current_term = request->term(); // update the current term and become follower
        raft_->role = Role::Follower;
        raft_->voted_for.reset();
        raft_->logger->info("Raft node {} received heartbeat from leader {} and becoming follower", raft_->id, request->leader_id());
    }
    // heartbeat
    else if(request->term() == raft_->current_term) {
        raft_->role = Role::Follower; // if we are candidate then move to follower
        raft_->logger->info("Raft node {} received heartbeat from leader {}", raft_->id, request->leader_id());
    }
    raft_->last_heartbeat = std::chrono::steady_clock::now();

    reply->set_term(raft_->current_term);
    reply->set_success(true);
    return grpc::Status::OK;
}

grpc::Status Raft::RaftServiceImpl::RequestVote(grpc::ServerContext *context,
                        const raftpb::RequestVoteRequest *request,
                        raftpb::RequestVoteReply *reply){
                          
    (void)context;
    std::lock_guard<std::mutex> lock(raft_->mtx);
    
    // the node requesting vote is stale
    if(request->term() < raft_->current_term){ 
        raft_->logger->info("Raft node {} denying vote to {}", raft_->id, request->candidate_id());
        reply->set_term(raft_->current_term);
        reply->set_vote_granted(false);
        return grpc::Status::OK;
    }
    else if(request->term() > raft_->current_term){ // the node requesting vote is in higher term, we should update our term and reset vote
        raft_->current_term = request->term();
        raft_->voted_for.reset();
        raft_->role = Role::Follower;
    }

    // if we have already voted for someone else in this term, deny vote
    if(raft_->voted_for.has_value() && raft_->voted_for.value() != request->candidate_id()){
        raft_->logger->info("Raft node {} denying vote to {}", raft_->id, request->candidate_id());
        reply->set_term(raft_->current_term);
        reply->set_vote_granted(false);
        return grpc::Status::OK;
    }
    //check candidate log -> skip for now since we don't have log yet

    // grant vote
    raft_->voted_for = request->candidate_id();
    raft_->last_heartbeat = std::chrono::steady_clock::now(); // reset election timeout
    raft_->logger->info("Raft node {} granting vote to {}", raft_->id, request->candidate_id());
    reply->set_term(raft_->current_term);
    reply->set_vote_granted(true);
    return grpc::Status::OK;
}

} // namespace rafty
