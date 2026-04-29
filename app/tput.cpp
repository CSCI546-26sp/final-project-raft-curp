#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <filesystem>
#include <format>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
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
static constexpr int OPS_PER_CLIENT = 1000;
static constexpr int KEYSPACE = 1000;

ABSL_FLAG(int, raft_node_verb, 0, "Raft node verbosity level (0/1/2)");
ABSL_FLAG(bool, ddb, false, "Enable DDB for debugging.");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1", "Host IP for DDB.");
ABSL_FLAG(bool, wait_for_attach, true, "Wait for DDB attach.");
ABSL_FLAG(bool, ddb_app_wrapper, true, "Use DDB app wrapper.");

ABSL_FLAG(std::string, bin, "./kv_node", "Path to kv_node binary");
ABSL_FLAG(int, fail_type, 0, "Failure type: 0 (failure), 1 (partition)");

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

static std::unique_ptr<RaftTestCtrl>
start_cluster(std::vector<std::string> &kv_addrs,
              std::shared_ptr<spdlog::logger> logger,
              const toolings::DDBConfig &ddb_conf) {
  kv_addrs.clear();

  auto insts = ConfigGen::gen_local_instances(NUM_NODES, RAFT_BASE_PORT);
  std::vector<rafty::Config> configs;
  std::unordered_map<uint64_t, uint64_t> node_tester_ports;
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
  auto c = std::make_unique<RaftTestCtrl>(
      configs, node_tester_ports, absl::GetFlag(FLAGS_bin), ctrl_addr,
      absl::GetFlag(FLAGS_fail_type), absl::GetFlag(FLAGS_raft_node_verb),
      logger, ddb_conf);

  c->register_applier_handler([](testerpb::ApplyResult m) -> void { (void)m; });
  c->run();
  std::this_thread::sleep_for(std::chrono::seconds(2));
  return c;
}

static void prepopulate(kv::KvClient &client) {
  for (int i = 1; i <= KEYSPACE; ++i) {
    client.put("key_" + std::to_string(i), "v");
  }
}

int main(int argc, char **argv) {
  if (setup_signal() != 0) {
    return 1;
  }
  absl::ParseCommandLine(argc, argv);
  rafty::utils::init_logger();

  if (argc < 3) {
    std::cerr << "Usage: ./tput <MaxClientCount> <PutRatio>\n";
    return 1;
  }

  int max_clients = std::stoi(argv[1]);
  int put_ratio = std::stoi(argv[2]);
  if (max_clients < 1) {
    std::cerr << "MaxClientCount must be >= 1\n";
    return 1;
  }
  if (put_ratio < 0 || put_ratio > 100) {
    std::cerr << "PutRatio must be in [0,100]\n";
    return 1;
  }

  toolings::DDBConfig ddb_conf{
      .enable_ddb = absl::GetFlag(FLAGS_ddb),
      .ddb_host_ip = absl::GetFlag(FLAGS_ddb_host_ip),
      .wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach),
      .ddb_app_wrapper = absl::GetFlag(FLAGS_ddb_app_wrapper)};

  auto logger_name = "tput";
  auto logger = spdlog::get(logger_name);
  if (!logger) {
    logger =
        spdlog::basic_logger_mt(logger_name, std::format("logs/{}.log", logger_name), true);
  }

  // Same directory as this binary (typically `build/app/`), matches `bench/lat-tput.py`.
  std::filesystem::path exe_dir;
  try {
    exe_dir = std::filesystem::canonical(std::filesystem::path(argv[0])).parent_path();
  } catch (...) {
    exe_dir = std::filesystem::current_path();
  }
  const auto result_path = exe_dir / "result.txt";

  std::ofstream out(result_path, std::ios::out | std::ios::trunc);
  if (!out) {
    std::cerr << "Failed to open result file: " << result_path.string() << "\n";
    return 1;
  }
  out << "##################################################################################\n";
  out << "# clientCount      latAvg       latP50       latP90       latP99        throughput\n";
  out << "#                    (ms)         (ms)         (ms)         (ms)         (ops/sec)\n";
  out << "----------------------------------------------------------------------------------\n";

  for (int clients = 1; clients <= max_clients && keep_running.load(std::memory_order_relaxed);
       clients *= 2) {
    std::vector<std::string> kv_addrs;
    ctrl = start_cluster(kv_addrs, logger, ddb_conf);

    kv::KvClient prep_client(kv_addrs);
    prepopulate(prep_client);

    std::atomic<uint64_t> total_ops{0};
    std::vector<std::vector<double>> per_thread_lat(clients);
    std::vector<std::thread> threads;
    threads.reserve(clients);

    auto bench_start = std::chrono::steady_clock::now();
    for (int t = 0; t < clients; ++t) {
      threads.emplace_back([&, t]() {
        kv::KvClient client(kv_addrs);
        per_thread_lat[t].reserve(OPS_PER_CLIENT);

        std::mt19937 rng(static_cast<uint32_t>(getpid()) ^ (t * 7919U) ^
                         static_cast<uint32_t>(
                             std::chrono::steady_clock::now().time_since_epoch().count()));
        std::uniform_int_distribution<int> key_dist(1, KEYSPACE);
        std::uniform_int_distribution<int> op_dist(1, 100);

        for (int i = 0; i < OPS_PER_CLIENT &&
                            keep_running.load(std::memory_order_relaxed);
             ++i) {
          int k = key_dist(rng);
          std::string key = "key_" + std::to_string(k);

          auto start = std::chrono::steady_clock::now();
          int roll = op_dist(rng);
          if (roll <= put_ratio) {
            client.put(key, "v");
          } else {
            (void)client.get(key);
          }
          auto end = std::chrono::steady_clock::now();

          auto dur = std::chrono::duration_cast<
              std::chrono::duration<double, std::milli>>(end - start);
          per_thread_lat[t].push_back(dur.count());
          total_ops.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }

    for (auto &th : threads) {
      th.join();
    }
    auto bench_end = std::chrono::steady_clock::now();

    double seconds = std::chrono::duration_cast<std::chrono::duration<double>>(
                         bench_end - bench_start)
                         .count();
    double throughput = seconds <= 0.0
                            ? 0.0
                            : static_cast<double>(total_ops.load()) / seconds;

    std::vector<double> all_lat;
    all_lat.reserve(static_cast<size_t>(clients) * OPS_PER_CLIENT);
    for (auto &v : per_thread_lat) {
      all_lat.insert(all_lat.end(), v.begin(), v.end());
    }

    double avg =
        all_lat.empty()
            ? 0.0
            : (std::accumulate(all_lat.begin(), all_lat.end(), 0.0) /
               static_cast<double>(all_lat.size()));
    double p50 = percentile_ms(all_lat, 0.50);
    double p90 = percentile_ms(all_lat, 0.90);
    double p99 = percentile_ms(all_lat, 0.99);

    out << std::setw(13) << clients << std::fixed << std::setprecision(2)
        << std::setw(12) << avg << std::setw(13) << p50 << std::setw(13) << p90
        << std::setw(13) << p99 << std::setw(19) << std::setprecision(0)
        << throughput << "\n";
    out.flush();

    if (ctrl) {
      ctrl->kill();
      ctrl.reset();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  return 0;
}