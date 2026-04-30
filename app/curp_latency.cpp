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

#include "curp_harness.hpp"

static constexpr uint64_t OPS = 1000;

ABSL_FLAG(int, raft_node_verb, 0, "Raft node verbosity level (0/1/2)");
ABSL_FLAG(bool, ddb, false, "Enable DDB for debugging.");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1", "Host IP for DDB.");
ABSL_FLAG(bool, wait_for_attach, true, "Wait for DDB attach.");
ABSL_FLAG(bool, ddb_app_wrapper, false, "Use DDB app wrapper.");
ABSL_FLAG(std::string, bin, "./kv_node", "Path to kv_node binary");
ABSL_FLAG(int, fail_type, 0, "Failure type: 0 (failure), 1 (partition)");
ABSL_FLAG(std::string, op, "put", "Operation: put or append");
ABSL_FLAG(bool, curp, false, "Use CURP fast-path instead of normal Raft path");

std::atomic<bool> keep_running(true);

void handle_sigint(int sig) {
  (void)sig;
  keep_running.store(false, std::memory_order_seq_cst);
}

int setup_signal() {
  struct sigaction sa;
  sa.sa_handler = handle_sigint;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;
  if (sigaction(SIGINT, &sa, NULL) == -1) {
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

  toolings::DDBConfig ddb_conf{
      .enable_ddb      = absl::GetFlag(FLAGS_ddb),
      .ddb_host_ip     = absl::GetFlag(FLAGS_ddb_host_ip),
      .wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach),
      .ddb_app_wrapper = absl::GetFlag(FLAGS_ddb_app_wrapper)};

  auto logger_name = "curp_latency";
  auto logger = spdlog::get(logger_name);
  if (!logger) {
    logger = spdlog::basic_logger_mt(
        logger_name, std::format("logs/{}.log", logger_name), true);
  }

  CurpCluster cluster(
      absl::GetFlag(FLAGS_bin),
      logger,
      absl::GetFlag(FLAGS_fail_type),
      absl::GetFlag(FLAGS_raft_node_verb),
      ddb_conf);
  cluster.start();

  kv::KvClient client(cluster.kv_addrs());

  std::vector<double> lat_ms;
  lat_ms.reserve(OPS);

  const std::string op = absl::GetFlag(FLAGS_op);
  const bool use_curp  = absl::GetFlag(FLAGS_curp);

  for (uint64_t i = 0; i < OPS && keep_running.load(std::memory_order_relaxed); ++i) {
    auto start = std::chrono::steady_clock::now();

    if (use_curp) {
      if (op == "append") {
        client.append_curp("key", "x");
      } else {
        client.put_curp("key", "value");
      }
    } else {
      if (op == "append") {
        client.append("key", "x");
      } else {
        client.put("key", "value");
      }
    }

    auto end = std::chrono::steady_clock::now();
    lat_ms.push_back(
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
            end - start).count());
  }

  cluster.stop();

  double avg = 0.0;
  if (!lat_ms.empty()) {
    avg = std::accumulate(lat_ms.begin(), lat_ms.end(), 0.0) /
          static_cast<double>(lat_ms.size());
  }

  double p50 = percentile_ms(lat_ms, 0.50);
  double p99 = percentile_ms(lat_ms, 0.99);

  double hit_rate = client.fast_path_hit_rate() * 100.0;

  std::cout << "######################################################################\n";
  std::cout << "#      latAvg       latP50      latP99   fast-path%  (5-node cluster)\n";
  std::cout << "#        (ms)         (ms)        (ms)\n";
  std::cout << "----------------------------------------------------------------------\n";
  std::cout << std::fixed << std::setprecision(2)
            << std::setw(12) << avg
            << std::setw(13) << p50
            << std::setw(12) << p99
            << std::setw(12) << hit_rate << "%\n";

  std::cout << "\nFast-path hits : " << client.fast_path_hits()
            << " / " << client.total_write_ops() << " ops\n";

  return 0;
}
