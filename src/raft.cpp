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
  // initiliaze log with dummy entry at index 0
  raftpb::Entry dummy_entry;
  dummy_entry.set_term(0);
  dummy_entry.set_index(0);
  dummy_entry.set_command("");
  this->log_entries.push_back(dummy_entry); // index 0 is dummy entry
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
  std::lock_guard<std::mutex> lock(this->mtx);

  ProposalResult res;
  res.term = this->current_term; // whatever term we are on
  res.is_leader = (this->role == Role::Leader);

  if(!res.is_leader) {
    res.index = 0;
    return res; // only leader can accept proposals
  }

  raftpb::Entry new_entry;
  new_entry.set_term(this->current_term);
  new_entry.set_index(this->log_entries.size()); // new entry index is current log size (0-based)
  new_entry.set_command(data);
  this->log_entries.push_back(new_entry);

  // update leader's own indices for the new entry
  this->match_index[id] = new_entry.index();
  this->next_index[id] = new_entry.index() + 1;

  res.index = new_entry.index();

  logger->info("Raft node {} proposed entry at index {} term {}", 
                 id, res.index, res.term);

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

    std::unique_lock<std::mutex> lock(this->mtx);

    if (!this->next_index.count(target_id)) {
      // default: next index starts at end of log (1-based, dummy at 0)
      this->next_index[target_id] = this->log_entries.size();
    }

    uint64_t next_idx = this->next_index[target_id];
    if (next_idx < 1) {
      next_idx = 1;
    }
    if (next_idx > this->log_entries.size()) {
      next_idx = this->log_entries.size();
    }
    uint64_t prev_log_index = next_idx - 1;
    uint64_t prev_log_term = this->log_entries[prev_log_index].term();
    uint64_t leader_commit = this->commit_index;

    std::vector<raftpb::Entry> entries_to_send(
            this->log_entries.begin() + next_idx,
            this->log_entries.end()
        );

    lock.unlock();

    std::thread([this, term, target_id, prev_log_index, prev_log_term, leader_commit, entries_to_send]() {
      auto& stub = this->peers_[target_id];
      raftpb::AppendEntriesRequest req;
      req.set_term(term);
      req.set_leader_id(this->id);
      req.set_prev_log_index(prev_log_index);
      req.set_prev_log_term(prev_log_term);
      req.set_leader_commit(leader_commit);

      // add log entries to replicate (empty for pure heartbeat)
      for (const auto &e : entries_to_send) {
        auto *added = req.add_entries();
        added->set_term(e.term());
        added->set_index(e.index());
        added->set_command(e.command());
      }

      auto context = this->create_context(target_id);
      raftpb::AppendEntriesReply reply;

      grpc::Status status = stub->AppendEntries(context.get(), req, &reply);
      if (!status.ok()) {
        return;
      }

      std::lock_guard<std::mutex> lock(this->mtx);
      if (this->role != Role::Leader || this->current_term != term) {
        return;
      }
      
      if (reply.term() > this->current_term) {
        this->current_term = reply.term();
        this->role = Role::Follower;
        this->voted_for.reset();
        this->last_heartbeat = std::chrono::steady_clock::now();
        logger->info("Raft node {} stepping down", id);
      }

      // reply handling
      if (reply.success()) {
        // all entries sent upto last idx replicated on follower
        uint64_t last_sent = prev_log_index + static_cast<uint64_t>(entries_to_send.size());
        this->next_index[target_id] = last_sent + 1;
        this->match_index[target_id] = last_sent; 

        logger->info("Raft node {} replicated log entries upto index {} on follower {}", 
          id, last_sent, target_id);

        // try to advance commit_index
        uint64_t last_idx = this->log_entries.size() - 1;
        size_t total_nodes = peer_addrs.size() + 1;
        size_t majority = total_nodes / 2 + 1;

        for (uint64_t N=this->commit_index+1; N<=last_idx; ++N) {
          size_t count = 0;

          // count leader itself
          auto self_it = this->match_index.find(this->id);
          if (self_it != this->match_index.end() && self_it->second >= N) {
            ++count;
          }

          // count followers
          for (const auto &kv: this->match_index) {
            if (kv.first == this->id) {
              continue;
            }
            if (kv.second >= N) {
              ++count;
            }
          }

          if (count >= majority && this->log_entries[N].term() == this->current_term) {
            this->commit_index = N;
            logger->info("Raft node {} advanced commit_index to {}", id, N);
          }
        }

        // apply newly committed entries
        while (this->last_applied < this->commit_index) {
          this->last_applied += 1;
          const auto &e = this->log_entries[this->last_applied];

          ApplyResult result;
          result.valid = true;
          result.data = e.command();
          result.index = e.index();
          this->apply(result);
          logger->info("Raft node {} applied committed entry index {} term {}", 
            id, e.index(), e.term());
        }
      } else {
        // backoff, move next_index one step back
        auto it = this->next_index.find(target_id);
        if (it != this->next_index.end() && it->second > 1) {
          it->second -= 1;
          logger->info("Raft node {} backing off next_index for follower {} to {}", 
            id, target_id, it->second);
        }
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
      req.set_last_log_index(this->log_entries.size() - 1); // last log index (0-based)
      req.set_last_log_term(this->log_entries.back().term()); // last log term

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

          uint64_t last_idx = this->log_entries.size() - 1;

          // self
          this->match_index[id] = last_idx;
          this->next_index[id] = last_idx + 1;

          // peers
          for(auto &p: peers_) {
            uint64_t peer_id = p.first;
            this->next_index[peer_id] = last_idx + 1;
            this->match_index[peer_id] = 0;
            //this->send_heartbeats(this->current_term); // send initial heartbeats immediately after becoming leader
          }
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

    // helper lambda() to get last log index (0 is dummy)
    auto last_log_index = [&]() -> uint64_t {
      return raft_->log_entries.size() > 0 ? static_cast<uint64_t>(raft_->log_entries.size() - 1) : 0;
    };

    // 1. log consistency check
    uint64_t prev_index = request->prev_log_index();
    uint64_t prev_term = request->prev_log_term();

    if (prev_index > last_log_index()) { // if we are missing entry at prev_index, reject
      raft_->logger->info("Raft node {} rejecting AppendEntries due to missing entry at index {} (last log index: {})", 
        raft_->id, prev_index, last_log_index());
      reply->set_term(raft_->current_term);
      reply->set_success(false);
      return grpc::Status::OK;
    }

    if (prev_index > 0) { // if prev_index > 0, check that term matches
      const auto &prev_entry = raft_->log_entries[prev_index];
      if (prev_entry.term() != prev_term) {
        raft_->logger->info("Raft node {} rejecting AppendEntries due to term mismatch at index {} (expected {}, got {}) (last log index: {})", 
          raft_->id, prev_index, prev_term, prev_entry.term(), last_log_index());
        reply->set_term(raft_->current_term);
        reply->set_success(false);
        return grpc::Status::OK;
      }
    }

    // 2. append/overwrite log entries from leader
    uint64_t insert_index = prev_index + 1;
    for (int i=0; i<request->entries_size(); ++i, ++insert_index) {
      const auto &incoming = request->entries(i);

      if (insert_index < raft_->log_entries.size()) { // delete entry if we already have it with different term
        if (raft_->log_entries[insert_index].term() != incoming.term()) {
          raft_->log_entries.erase(raft_->log_entries.begin() + static_cast<std::ptrdiff_t>(insert_index), raft_->log_entries.end());
        } else {
          continue; // same term & idx already matches, skip
        }
      }

      raftpb::Entry e; // append new entry
      e.set_term(incoming.term());
      e.set_index(incoming.index());
      e.set_command(incoming.command());
      raft_->logger->info("Raft node {} appending log entry index {} term {} from leader {}",
        raft_->id, incoming.index(), incoming.term(), request->leader_id());
      raft_->log_entries.push_back(std::move(e));
    }

    // 3. update commit_index from leader_commit
    uint64_t last_idx_after_append = last_log_index();
    if (request->leader_commit() > raft_->commit_index) {
      raft_->commit_index = std::min<uint64_t>(request->leader_commit(), last_idx_after_append);
    }

    // 4. apply newly committed entries
    while (raft_->last_applied < raft_->commit_index) {
      raft_->last_applied += 1;
      const auto &e = raft_->log_entries[raft_->last_applied];

      ApplyResult result;
      result.valid = true;
      result.data = e.command();
      result.index = e.index(); // should equal last_applied
      raft_->logger->info("Raft node {} applying committed entry index {} term {}",
        raft_->id, e.index(), e.term());
      raft_->apply(result);
    }

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
    uint64_t my_last_index = raft_->log_entries.size() - 1;
    uint64_t my_last_term = raft_->log_entries.back().term();

    bool candidate_log_up_to_date = (request->last_log_term() > my_last_term) ||
                        (request->last_log_term() == my_last_term && request->last_log_index() >= my_last_index);  

    if(!candidate_log_up_to_date){
        raft_->logger->info("Raft node {} denying vote to {} due to log not up-to-date", raft_->id, request->candidate_id());
        reply->set_term(raft_->current_term);
        reply->set_vote_granted(false);
        return grpc::Status::OK;
    }

    // grant vote
    raft_->voted_for = request->candidate_id();
    raft_->last_heartbeat = std::chrono::steady_clock::now(); // reset election timeout
    raft_->logger->info("Raft node {} granting vote to {}", raft_->id, request->candidate_id());
    reply->set_term(raft_->current_term);
    reply->set_vote_granted(true);
    return grpc::Status::OK;
}

} // namespace rafty
