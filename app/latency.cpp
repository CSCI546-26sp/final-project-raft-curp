#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <format>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <csignal>
#include <unistd.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "common/logger.hpp"
#include "kv/kv_client.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"

#include <ddb/integration.hpp>

using namespace toolings;

static constexpr uint64_t KV_PORT_OFFSET = 1000;
static constexpr uint64_t NUM_NODES = 3;
static constexpr uint64_t RAFT_BASE_PORT = 50050;
static constexpr uint64_t OPS = 1000;

ABSL_FLAG(int, raft_node_verb, 0, "Raft node verbosity level (0/1/2)");
ABSL_FLAG(bool, ddb, false, "Enable DDB for debugging.");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1", "Host IP for DDB.");
ABSL_FLAG(bool, wait_for_attach, true, "Wait for DDB attach.");
ABSL_FLAG(bool, ddb_app_wrapper, true, "Use DDB app wrapper.");

ABSL_FLAG(std::string, bin, "./kv_node", "Path to kv_node binary");
ABSL_FLAG(int, fail_type, 0, "Failure type: 0 (failure), 1 (partition)");
ABSL_FLAG(std::string, op, "put", "Operation: put or append");

static std::unique_ptr<RaftTestCtrl> ctrl = nullptr;
std::atomic<bool> keep_running(true);

void handle_sigint(int sig) {
  (void)sig;
  std::cout << "\nCaught SIGINT, cleaning up..." << std::endl;
  if (ctrl) {
    ctrl->kill();
    ctrl.reset();
  }
  keep_running.store(false, std::memory_order_seq_cst);
}

int setup_signal() {
  struct sigaction sa;
  sa.sa_handler = handle_sigint;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  if (sigaction(SIGINT, &sa, NULL) == -1) {
    perror("sigaction");
    return 1;
  }
  return 0;
}

static double percentile_ms(std::vector<double> v, double p) {
  if (v.empty()) {
    return 0.0;
  }
  std::sort(v.begin(), v.end());
  double clamped = std::min(1.0, std::max(0.0, p));
  size_t idx = static_cast<size_t>(clamped * static_cast<double>(v.size() - 1));
  return v[idx];
}

int main(int argc, char **argv) {
  if (setup_signal() != 0) {
    return 1;
  }
  absl::ParseCommandLine(argc, argv);
  rafty::utils::init_logger();

  // DDB config (optional)
  toolings::DDBConfig ddb_conf{
      .enable_ddb = absl::GetFlag(FLAGS_ddb),
      .ddb_host_ip = absl::GetFlag(FLAGS_ddb_host_ip),
      .wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach),
      .ddb_app_wrapper = absl::GetFlag(FLAGS_ddb_app_wrapper)};

  // Logger
  auto logger_name = "latency";
  auto logger = spdlog::get(logger_name);
  if (!logger) {
    logger = spdlog::basic_logger_mt(
        logger_name, std::format("logs/{}.log", logger_name), true);
  }

  // Generate configs
  auto insts = ConfigGen::gen_local_instances(NUM_NODES, RAFT_BASE_PORT);
  std::vector<rafty::Config> configs;
  std::unordered_map<uint64_t, uint64_t> node_tester_ports;
  std::vector<std::string> kv_addrs;
  uint64_t tester_port = 55001;

  for (const auto &inst : insts) {
    std::map<uint64_t, std::string> peer_addrs;
    for (const auto &peer : insts) {
      if (peer.id == inst.id) {
        continue;
      }
      peer_addrs[peer.id] = peer.external_addr;
    }
    rafty::Config config = {
        .id = inst.id, .addr = inst.listening_addr, .peer_addrs = peer_addrs};
    configs.push_back(config);
    node_tester_ports[inst.id] = tester_port++;
    kv_addrs.push_back("localhost:" +
                       std::to_string(inst.port + KV_PORT_OFFSET));
  }

  const std::string ctrl_addr = "0.0.0.0:55000";
  ctrl = std::make_unique<RaftTestCtrl>(
      configs, node_tester_ports, absl::GetFlag(FLAGS_bin), ctrl_addr,
      absl::GetFlag(FLAGS_fail_type), absl::GetFlag(FLAGS_raft_node_verb),
      logger, ddb_conf);

  // KV server handles apply internally; tester applier is no-op
  ctrl->register_applier_handler([](testerpb::ApplyResult m) -> void {
    (void)m;
  });

  ctrl->run();
  std::this_thread::sleep_for(std::chrono::seconds(2));

  kv::KvClient client(kv_addrs);

  std::vector<double> lat_ms;
  lat_ms.reserve(OPS);

  std::string op = absl::GetFlag(FLAGS_op);
  for (uint64_t i = 0; i < OPS && keep_running.load(std::memory_order_relaxed);
       ++i) {
    auto start = std::chrono::steady_clock::now();
    if (op == "append") {
      client.append("key", "x");
    } else {
      client.put("key", "value");
    }
    auto end = std::chrono::steady_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
        end - start);
    lat_ms.push_back(dur.count());
  }

  if (ctrl) {
    ctrl->kill();
    ctrl.reset();
  }

  double avg =
      lat_ms.empty()
          ? 0.0
          : (std::accumulate(lat_ms.begin(), lat_ms.end(), 0.0) /
             static_cast<double>(lat_ms.size()));
  double p50 = percentile_ms(lat_ms, 0.50);
  double p99 = percentile_ms(lat_ms, 0.99);

  std::cout << "######################################\n";
  std::cout << "#      latAvg       latP50      latP99\n";
  std::cout << "#        (ms)         (ms)        (ms)\n";
  std::cout << "--------------------------------------\n";
  std::cout << std::fixed << std::setprecision(2) << std::setw(12) << avg
            << std::setw(13) << p50 << std::setw(12) << p99 << "\n";

  return 0;
}