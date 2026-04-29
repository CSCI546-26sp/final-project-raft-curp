#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

#include <chrono>
#include <thread>
#include <vector>

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

Raft::~Raft() {
  this->kill();
  this->signal_replication();
  for (auto &t : this->repl_workers_) {
    if (t.joinable()) {
      t.join();
    }
  }
  this->stop_server();
}

void Raft::set_on_become_leader(std::function<void(uint64_t)> cb) {
  std::lock_guard<std::mutex> lock(this->mtx);
  this->on_become_leader_ = std::move(cb);
}

uint64_t Raft::get_current_term() const {
  std::lock_guard<std::mutex> lock(this->mtx);
  return this->current_term;
}

uint64_t Raft::get_id() const { return this->id; }

std::string Raft::get_listening_addr() const { return this->listening_addr; }

std::map<uint64_t, std::string> Raft::get_peer_addrs() const {
  return this->peer_addrs;
}

void Raft::signal_replication() {
  {
    std::lock_guard<std::mutex> lk(this->repl_mu_);
    this->repl_epoch_.fetch_add(1, std::memory_order_release);
  }
  this->repl_cv_.notify_all();
}

void Raft::start_replication_workers() {
  bool expected = false;
  if (!this->repl_started_.compare_exchange_strong(expected, true)) {
    return;
  }

  for (auto &p : peers_) {
    uint64_t target_id = p.first;
    if (!p.second) {
      continue;
    }
    this->repl_workers_.emplace_back(
        [this, target_id]() { this->replication_worker(target_id); });
  }
}

void Raft::replication_worker(uint64_t target_id) {
  auto next_hb = std::chrono::steady_clock::now();
  uint64_t seen_epoch = this->repl_epoch_.load(std::memory_order_acquire);

  while (!this->is_dead()) {
    uint64_t term = 0;
    bool leader = false;
    uint64_t last_idx = 0;
    uint64_t next_idx = 0;
    {
      std::lock_guard<std::mutex> lock(this->mtx);
      leader = (this->role == Role::Leader);
      term = this->current_term;
      last_idx = this->log_entries.empty()
                     ? 0
                     : static_cast<uint64_t>(this->log_entries.size() - 1);
      auto it = this->next_index.find(target_id);
      next_idx = (it == this->next_index.end()) ? (last_idx + 1) : it->second;
    }

    if (!leader) {
      std::unique_lock<std::mutex> lk(this->repl_mu_);
      this->repl_cv_.wait_for(lk, std::chrono::milliseconds(10), [&] {
        return this->is_dead() ||
               this->repl_epoch_.load(std::memory_order_acquire) != seen_epoch;
      });
      seen_epoch = this->repl_epoch_.load(std::memory_order_acquire);
      continue;
    }

    auto now = std::chrono::steady_clock::now();
    const bool have_entries = (next_idx <= last_idx);
    const bool heartbeat_due = (now >= next_hb);

    if (have_entries || heartbeat_due) {
      (void)this->replicate_to_follower(target_id, term);
      next_hb = std::chrono::steady_clock::now() + heartbeat_interval;
      continue;
    }

    std::unique_lock<std::mutex> lk(this->repl_mu_);
    this->repl_cv_.wait_until(lk, next_hb, [&] {
      return this->is_dead() ||
             this->repl_epoch_.load(std::memory_order_acquire) != seen_epoch;
    });
    seen_epoch = this->repl_epoch_.load(std::memory_order_acquire);
  }
}

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
          if (now >= next_heartbeat) {
            next_heartbeat = now + heartbeat_interval;

            auto term = this->current_term;

            // Kick background replicators (they also do periodic heartbeats).
            lock.unlock();
            (void)term;
            this->signal_replication();
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

      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    
    logger->info("Raft node {} run loop exit", id);
  }).detach();

  // Background replication workers (one per follower).
  this->start_replication_workers();
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
  ProposalResult res;

  {
    std::lock_guard<std::mutex> lock(this->mtx);

    res.term = this->current_term;
    res.is_leader = (this->role == Role::Leader);

    if (!res.is_leader) {
      res.index = 0;
      return res;
    }

    raftpb::Entry new_entry;
    new_entry.set_term(this->current_term);
    new_entry.set_index(this->log_entries.size());
    new_entry.set_command(data);
    this->log_entries.push_back(new_entry);

    this->match_index[id] = new_entry.index();
    this->next_index[id] = new_entry.index() + 1;

    res.index = new_entry.index();

    logger->debug("Raft node {} proposed entry at index {} term {}", id, res.index, res.term);
  }

  // Replication happens on background per-follower workers.
  this->signal_replication();

  return res;
}

ProposalResult Raft::propose_sync(const std::string &data) {
  // TODO: lab 3
  return propose(data);
}

bool Raft::replicate_to_follower(uint64_t target_id, uint64_t term) {
  auto *stub = this->peers_[target_id].get();
  if (!stub) {
    return false;
  }

  uint64_t prev_log_index = 0;
  uint64_t prev_log_term = 0;
  uint64_t leader_commit = 0;
  std::vector<raftpb::Entry> entries_to_send;

  {
    std::unique_lock<std::mutex> lock(this->mtx);

    if (this->role != Role::Leader || this->current_term != term) {
      return false;
    }

    if (!this->next_index.count(target_id)) {
      this->next_index[target_id] = this->log_entries.size();
    }

    uint64_t next_idx = this->next_index[target_id];
    if (next_idx < 1) {
      next_idx = 1;
    }
    if (next_idx > this->log_entries.size()) {
      next_idx = this->log_entries.size();
    }
    prev_log_index = next_idx - 1;
    prev_log_term = this->log_entries[prev_log_index].term();
    leader_commit = this->commit_index;

    entries_to_send.assign(this->log_entries.begin() + static_cast<std::ptrdiff_t>(next_idx),
                           this->log_entries.end());
    lock.unlock();
  }

  raftpb::AppendEntriesRequest req;
  req.set_term(term);
  req.set_leader_id(this->id);
  req.set_prev_log_index(prev_log_index);
  req.set_prev_log_term(prev_log_term);
  req.set_leader_commit(leader_commit);

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
    return false;
  }

  std::lock_guard<std::mutex> lock(this->mtx);
  if (this->role != Role::Leader || this->current_term != term) {
    return false;
  }

  if (reply.term() > this->current_term) {
    this->current_term = reply.term();
    this->role = Role::Follower;
    this->voted_for.reset();
    this->last_heartbeat = std::chrono::steady_clock::now();
    logger->info("Raft node {} stepping down", id);
    return false;
  }

  if (reply.success()) {
    uint64_t last_sent =
        prev_log_index + static_cast<uint64_t>(entries_to_send.size());
    uint64_t prev_next = this->next_index[target_id];
    uint64_t prev_match = this->match_index.count(target_id) ? this->match_index[target_id] : 0;
    if ((last_sent + 1) > prev_next) {
      this->next_index[target_id] = (last_sent + 1);
    }
    if (last_sent > prev_match) {
      this->match_index[target_id] = last_sent;
    }

    logger->debug("Raft node {} replicated log entries upto index {} on follower {}", id,
                  last_sent, target_id);

    uint64_t last_idx = this->log_entries.size() - 1;
    size_t total_nodes = peer_addrs.size() + 1;
    size_t majority = total_nodes / 2 + 1;

    for (uint64_t N = this->commit_index + 1; N <= last_idx; ++N) {
      size_t count = 0;

      auto self_it = this->match_index.find(this->id);
      if (self_it != this->match_index.end() && self_it->second >= N) {
        ++count;
      }

      for (const auto &kv : this->match_index) {
        if (kv.first == this->id) {
          continue;
        }
        if (kv.second >= N) {
          ++count;
        }
      }

      if (count >= majority && this->log_entries[N].term() == this->current_term) {
        this->commit_index = N;
        logger->debug("Raft node {} advanced commit_index to {}", id, N);
        this->last_quorum_ack_ = std::chrono::steady_clock::now();
      }
    }

    while (this->last_applied < this->commit_index) {
      this->last_applied += 1;
      const auto &e = this->log_entries[this->last_applied];

      ApplyResult result;
      result.valid = true;
      result.data = e.command();
      result.index = e.index();
      this->apply(result);
      logger->debug("Raft node {} applied committed entry index {} term {}", id, e.index(),
                    e.term());
    }
    return true;
  }

  uint64_t cf_term = reply.conflict_term();
  uint64_t cf_index = reply.conflict_index();

  uint64_t candidate_next;
  if (cf_term == 0) {
    candidate_next = cf_index;
  } else {
    uint64_t new_next = cf_index;
    for (uint64_t i = static_cast<uint64_t>(this->log_entries.size()); i-- > 1;) {
      if (this->log_entries[i].term() == cf_term) {
        new_next = i + 1;
        break;
      }
    }
    candidate_next = new_next;
  }

  if (candidate_next < 1) {
    candidate_next = 1;
  }

  uint64_t old_next = this->next_index[target_id];
  if (candidate_next < old_next) {
    this->next_index[target_id] = candidate_next;
    logger->debug(
        "Raft node {} backing off next_index for follower {} to {} based on conflict term {} "
        "and index {}",
        id, target_id, this->next_index[target_id], cf_term, cf_index);
  }
  return false;
}

uint64_t Raft::get_commit_index() const {
  std::lock_guard<std::mutex> lock(this->mtx);
  return this->commit_index;
}

bool Raft::read_quorum_barrier(std::chrono::milliseconds timeout) {
  uint64_t term = 0;
  {
    std::lock_guard<std::mutex> lock(this->mtx);
    if (this->role != Role::Leader) {
      return false;
    }
    term = this->current_term;
  }

  const size_t total_nodes = peer_addrs.size() + 1;
  const size_t majority = total_nodes / 2 + 1;

  if (peers_.empty()) {
    return true;
  }

  const auto deadline = std::chrono::steady_clock::now() + timeout;
  // Confirm leadership by contacting a quorum. Avoid per-read thread creation:
  // for 3 nodes, we only need self + 1 follower.
  size_t ok = 1;
  for (auto &p : peers_) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    uint64_t target_id = p.first;
    if (!p.second) {
      continue;
    }
    if (this->replicate_to_follower(target_id, term)) {
      ++ok;
      if (ok >= majority) {
        break;
      }
    }
  }

  {
    std::lock_guard<std::mutex> lock(this->mtx);
    if (!(this->role == Role::Leader && this->current_term == term && ok >= majority)) {
      return false;
    }
  }
  
  uint64_t noop_idx = 0;
  bool need_noop = false;
  {
    std::lock_guard<std::mutex> lock(this->mtx);
    if (this->role != Role::Leader || this->current_term != term) {
      return false;
    }
    if (this->commit_index == 0 || this->log_entries[this->commit_index].term() != this->current_term) {
      need_noop = true;
    }
  }
  if (!need_noop) {
    return true;
  }

  // Append a NOOP entry in this term and replicate immediately.
  {
    std::lock_guard<std::mutex> lock(this->mtx);
    if (this->role != Role::Leader || this->current_term != term) {
      return false;
    }
    raftpb::Entry e;
    e.set_term(this->current_term);
    e.set_index(this->log_entries.size());
    e.set_command("NOOP\t\t\t0\t0");
    this->log_entries.push_back(e);
    this->match_index[this->id] = e.index();
    this->next_index[this->id] = e.index() + 1;
    noop_idx = e.index();
  }
  this->signal_replication();

  // Wait for NOOP to become committed (bounded by the same deadline).
  while (true) {
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;
    }
    {
      std::lock_guard<std::mutex> lock(this->mtx);
      if (this->role != Role::Leader || this->current_term != term) {
        return false;
      }
      if (this->commit_index >= noop_idx) {
        return true;
      }
    }
    std::this_thread::yield();
  }
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
    if (!p.second) {
      continue;
    }
    (void)this->replicate_to_follower(target_id, term);
  }
}

void Raft::send_heartbeats(uint64_t term, uint64_t min_commit_index) {
  for (auto &p : peers_) {
    uint64_t target_id = p.first;
    if (!p.second) {
      continue;
    }
    (void)this->replicate_to_follower(target_id, term);
    {
      std::lock_guard<std::mutex> lock(this->mtx);
      if (this->commit_index >= min_commit_index) {
        return;
      }
    }
  }
}

bool Raft::has_valid_lease() const {
  std::lock_guard<std::mutex> lock(mtx);
  if (role != Role::Leader) return false;
  auto elapsed = std::chrono::steady_clock::now() - last_quorum_ack_;
  return elapsed < std::chrono::milliseconds(100);
}

void Raft::send_request_votes(uint64_t term) {
  size_t total_nodes = peer_addrs.size() + 1;
  size_t majority = total_nodes / 2 + 1;

  uint64_t last_log_idx;
  uint64_t last_log_term;
  {
    std::lock_guard<std::mutex> lock(this->mtx);
    last_log_idx = this->log_entries.empty() ? 0 : static_cast<uint64_t>(this->log_entries.size() - 1);
    last_log_term = this->log_entries.empty() ? 0 : this->log_entries.back().term();
  }

  for (auto &p : peers_) {
    uint64_t target_id = p.first;
    auto *stub = p.second.get();
    if (!stub) {
      continue;
    }

    std::thread([this, term, target_id, majority, last_log_idx, last_log_term]() {
      auto& stub = this->peers_[target_id];
      raftpb::RequestVoteRequest req;
      req.set_term(term);
      req.set_candidate_id(id);
      req.set_last_log_index(last_log_idx);
      req.set_last_log_term(last_log_term);

      auto context = this->create_context(target_id);
      raftpb::RequestVoteReply reply;

      grpc::Status status = stub->RequestVote(context.get(), req, &reply);
      if (!status.ok()) {
        return;
      }

      std::function<void(uint64_t)> cb;
      bool became_leader = false;
      uint64_t became_term = 0;
      {
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

          cb = this->on_become_leader_;
          became_leader = true;
          became_term = this->current_term;
        }
      }
      }

      if (became_leader && cb) {
        cb(became_term);
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
        raft_->logger->debug("Raft node {} received heartbeat from leader {} and becoming follower",
                            raft_->id, request->leader_id());
    }
    // heartbeat
    else if(request->term() == raft_->current_term) {
        raft_->role = Role::Follower; // if we are candidate then move to follower
        raft_->logger->debug("Raft node {} received heartbeat from leader {}", raft_->id,
                             request->leader_id());
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
      raft_->logger->debug(
          "Raft node {} rejecting AppendEntries due to missing entry at index {} (last log index: {})",
          raft_->id, prev_index, last_log_index());
      reply->set_term(raft_->current_term);
      reply->set_success(false);
      reply->set_conflict_term(0);
      reply->set_conflict_index(raft_->log_entries.size()); // next index to append
      return grpc::Status::OK;
    }

    if (prev_index > 0) { // if prev_index > 0, check that term matches
      const auto &prev_entry = raft_->log_entries[prev_index];
      if (prev_entry.term() != prev_term) {
        raft_->logger->debug(
            "Raft node {} rejecting AppendEntries due to term mismatch at index {} (expected {}, got "
            "{}) (last log index: {})",
            raft_->id, prev_index, prev_term, prev_entry.term(), last_log_index());
        uint64_t conflict_term = prev_entry.term();
        uint64_t conflict_index = prev_index;
        while(conflict_index > 1 && raft_->log_entries[conflict_index-1].term() == conflict_term) {
          conflict_index -= 1;
        }
        reply->set_term(raft_->current_term);
        reply->set_success(false);
        reply->set_conflict_term(conflict_term);
        reply->set_conflict_index(conflict_index);
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
      raft_->logger->debug("Raft node {} appending log entry index {} term {} from leader {}",
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
      raft_->logger->debug("Raft node {} applying committed entry index {} term {}", raft_->id,
                           e.index(), e.term());
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

Raft::WitnessRecordResult Raft::witness_record(const std::string& op_type,
                               const std::string& key,
                               const std::string& value,
                               uint64_t client_id,
                               uint64_t seq_num) {
  std::lock_guard<std::mutex> lock(this->witness_mtx_);

  // Recovery mode
  if (this->witness_recovery_mode_) {
    return {true, 0};
  }

  if(op_type != "GET") {
    for(const auto & [idx, op] : unsynced_ops_) {
      if (op.key == key) {
        // Same key + at least one write = conflict, deny fast path
        logger->debug("Raft node {} witness CONFLICT on key '{}' "
                      "(existing op: {} by client {})",
                      id, key, op.op_type, op.client_id);
        return {true, 0};
      }
    }
  }

  uint64_t witness_idx = ++next_unsynced_index_;
  unsynced_ops_[witness_idx] = UnsyncedOp{key, value, op_type, client_id, seq_num};
  logger->debug("Raft node {} witness RECORD idx={} op={} key='{}' "
                "client={} seq={}",
                id, witness_idx, op_type, key, client_id, seq_num);
  return {false, witness_idx};
}

void Raft::witness_gc(uint64_t up_to) {
  std::lock_guard<std::mutex> lock(this->witness_mtx_);
  auto it = unsynced_ops_.begin();
  while (it != unsynced_ops_.end()) {
    if (it->first <= up_to) {
      logger->debug("Raft node {} witness GC idx={} key='{}'",
                    id, it->first, it->second.key);
      it = unsynced_ops_.erase(it);
    } else {
      ++it;
    }
  }
}

std::vector<Raft::UnsyncedOp> Raft::witness_get_recovery_data() {
  std::lock_guard<std::mutex> lock(this->witness_mtx_);
  std::vector<UnsyncedOp> result;
  result.reserve(unsynced_ops_.size());
  for (const auto& [idx, op] : unsynced_ops_) {
    result.push_back(op);
  }
  return result;
}

bool Raft::witness_has_unsynced_write(const std::string& key) {
  std::lock_guard<std::mutex> lock(this->witness_mtx_);
  for (const auto& [idx, op] : unsynced_ops_) {
    if (op.key == key && op.op_type != "GET") {
      return true;
    }
  }
  return false;
}

bool Raft::has_unsynced_ops() const {
    std::lock_guard<std::mutex> lock(this->witness_mtx_); // ← not mtx
    return !unsynced_ops_.empty();
}

void Raft::witness_enter_recovery() {
  std::lock_guard<std::mutex> lock(this->witness_mtx_);
  this->witness_recovery_mode_ = true;
}

void Raft::witness_exit_recovery() {
  std::lock_guard<std::mutex> lock(this->witness_mtx_);
  this->witness_recovery_mode_ = false;
}

bool Raft::witness_in_recovery() const {
  std::lock_guard<std::mutex> lock(this->witness_mtx_);
  return this->witness_recovery_mode_;
}

void Raft::witness_clear() {
  std::lock_guard<std::mutex> lock(this->witness_mtx_);
  unsynced_ops_.clear();
  next_unsynced_index_ = 1;
}

} // namespace rafty
