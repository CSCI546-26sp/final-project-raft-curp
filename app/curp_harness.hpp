#pragma once

#include <chrono>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/config.hpp"
#include "common/logger.hpp"
#include "kv/kv_client.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"


class CurpCluster {
public:
  static constexpr uint64_t NUM_NODES      = 5;
  static constexpr uint64_t RAFT_BASE_PORT = 50050;
  static constexpr uint64_t KV_PORT_OFFSET = 1000;
  static constexpr uint64_t TESTER_BASE    = 55001;

  explicit CurpCluster(std::string kv_node_bin,
                       std::shared_ptr<spdlog::logger> logger,
                       int fail_type = 0,
                       int verbosity = 0,
                       toolings::DDBConfig ddb_conf = {})
      : bin_(std::move(kv_node_bin)),
        logger_(std::move(logger)),
        fail_type_(fail_type),
        verbosity_(verbosity),
        ddb_conf_(std::move(ddb_conf)) {}

  ~CurpCluster() { stop(); }

  // Start all 5 nodes and wait for leader election .
  void start(std::chrono::milliseconds settle = std::chrono::seconds(2)) {
    kv_addrs_.clear();
    configs_.clear();
    node_tester_ports_.clear();
    node_ids_.clear();

    auto insts = toolings::ConfigGen::gen_local_instances(NUM_NODES, RAFT_BASE_PORT);
    uint64_t tester_port = TESTER_BASE;

    for (const auto &inst : insts) {
      std::map<uint64_t, std::string> peer_addrs;
      for (const auto &peer : insts) {
        if (peer.id == inst.id) continue;
        peer_addrs[peer.id] = peer.external_addr;
      }
      rafty::Config cfg{.id = inst.id,
                        .addr = inst.listening_addr,
                        .peer_addrs = peer_addrs};
      configs_.push_back(cfg);
      node_tester_ports_[inst.id] = tester_port++;
      kv_addrs_.push_back("localhost:" +
                           std::to_string(inst.port + KV_PORT_OFFSET));
      node_ids_.push_back(inst.id);
    }

    const std::string ctrl_addr = "0.0.0.0:55000";
    ctrl_ = std::make_unique<toolings::RaftTestCtrl>(
        configs_, node_tester_ports_, bin_, ctrl_addr,
        fail_type_, verbosity_, logger_,
        ddb_conf_); 

    ctrl_->register_applier_handler(
        [](testerpb::ApplyResult m) -> void { (void)m; });
    ctrl_->run();
    std::this_thread::sleep_for(settle);

    client_ = std::make_unique<kv::KvClient>(kv_addrs_);
  }

  
  void stop() {
    client_.reset();
    if (ctrl_) {
      ctrl_->kill();
      ctrl_.reset();
    }
  }

  // Disconnect a specific node from the cluster to simulates partition/crash
  void kill_node(uint64_t node_id) {
    ctrl_->disconnect({node_id});
  }

  // Reconnect a previously disconnected node
  void revive_node(uint64_t node_id) {
    ctrl_->reconnect({node_id});
  }

  // Disconnect multiple nodes at once
  void kill_nodes(std::vector<uint64_t> ids) {
    ctrl_->disconnect(ids);
  }

  // Reconnect multiple nodes at once
  void revive_nodes(std::vector<uint64_t> ids) {
    ctrl_->reconnect(ids);
  }

  kv::KvClient &client() {
    if (!client_) throw std::runtime_error("CurpCluster not started");
    return *client_;
  }

  // Build a fresh KvClient
  kv::KvClient make_client() const {
    return kv::KvClient(kv_addrs_);
  }

  const std::vector<std::string> &kv_addrs() const { return kv_addrs_; }
  const std::vector<uint64_t> &node_ids() const { return node_ids_; }

private:
  std::string bin_;
  std::shared_ptr<spdlog::logger> logger_;
  int fail_type_;
  int verbosity_;
  toolings::DDBConfig ddb_conf_;  // passed to RaftTestCtrl

  std::vector<rafty::Config> configs_;
  std::unordered_map<uint64_t, uint64_t> node_tester_ports_;
  std::vector<std::string> kv_addrs_;
  std::vector<uint64_t> node_ids_;

  std::unique_ptr<toolings::RaftTestCtrl> ctrl_;
  std::unique_ptr<kv::KvClient> client_;
};
