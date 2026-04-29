#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <format>
#include <fstream>
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

#include "curp_harness.hpp"
#include "zipfian.hpp"

static constexpr int OPS_PER_CLIENT = 1000;
static constexpr int KEYSPACE       = 1000;

ABSL_FLAG(int, raft_node_verb, 0, "Raft node verbosity level (0/1/2)");
ABSL_FLAG(bool, ddb, false, "Enable DDB.");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1", "Host IP for DDB.");
ABSL_FLAG(bool, wait_for_attach, true, "Wait for DDB attach.");
ABSL_FLAG(bool, ddb_app_wrapper, false, "Use DDB app wrapper.");
ABSL_FLAG(std::string, bin, "./kv_node", "Path to kv_node binary");
ABSL_FLAG(int, fail_type, 0, "Failure type.");
ABSL_FLAG(bool, curp, false, "Use CURP fast-path for writes.");
ABSL_FLAG(double, zipf_theta, 0.0,
          "Zipfian skew theta in (0,1). 0 = uniform (default).");

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
    std::cerr << "Usage: ./curp_tput <MaxClientCount> <PutRatio> [flags]\n";
    return 1;
  }

  int max_clients = std::stoi(argv[1]);
  int put_ratio   = std::stoi(argv[2]);
  if (max_clients < 1 || put_ratio < 0 || put_ratio > 100) {
    std::cerr << "Invalid arguments\n";
    return 1;
  }

  const bool use_curp    = absl::GetFlag(FLAGS_curp);
  const double zip_theta = absl::GetFlag(FLAGS_zipf_theta);
  const bool use_zipf    = (zip_theta > 0.0);

  std::unique_ptr<ZipfianGenerator> zipf;
  if (use_zipf) {
    zipf = std::make_unique<ZipfianGenerator>(KEYSPACE, zip_theta);
  }

  toolings::DDBConfig ddb_conf{
      .enable_ddb      = absl::GetFlag(FLAGS_ddb),
      .ddb_host_ip     = absl::GetFlag(FLAGS_ddb_host_ip),
      .wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach),
      .ddb_app_wrapper = absl::GetFlag(FLAGS_ddb_app_wrapper)};

  auto logger_name = "curp_tput";
  auto logger = spdlog::get(logger_name);
  if (!logger) {
    logger = spdlog::basic_logger_mt(
        logger_name, std::format("logs/{}.log", logger_name), true);
  }

  std::filesystem::path exe_dir;
  try {
    exe_dir = std::filesystem::canonical(
                  std::filesystem::path(argv[0])).parent_path();
  } catch (...) {
    exe_dir = std::filesystem::current_path();
  }

  const auto result_path = exe_dir / "result.txt";
  std::ofstream out(result_path, std::ios::out | std::ios::trunc);
  if (!out) {
    std::cerr << "Failed to open result file: " << result_path.string() << "\n";
    return 1;
  }

  out << "##################################################################################################\n";
  out << "# clientCount      latAvg       latP50       latP90       latP99        throughput   fast-path%\n";
  out << "#                    (ms)         (ms)         (ms)         (ms)         (ops/sec)\n";
  out << "--------------------------------------------------------------------------------------------------\n";

  for (int clients = 1;
       clients <= max_clients && keep_running.load(std::memory_order_relaxed);
       clients *= 2) {

    CurpCluster cluster(
        absl::GetFlag(FLAGS_bin),
        logger,
        absl::GetFlag(FLAGS_fail_type),
        absl::GetFlag(FLAGS_raft_node_verb),
        ddb_conf);
    cluster.start();

    {
      auto prep = cluster.make_client();
      prepopulate(prep);
    }

    std::atomic<uint64_t> total_ops{0};
    std::atomic<uint64_t> fast_hits{0};
    std::vector<std::vector<double>> per_thread_lat(clients);
    std::vector<std::thread> threads;
    threads.reserve(clients);

    auto bench_start = std::chrono::steady_clock::now();

    for (int t = 0; t < clients; ++t) {
      threads.emplace_back([&, t]() {
        kv::KvClient client(cluster.kv_addrs());
        per_thread_lat[t].reserve(OPS_PER_CLIENT);

        std::mt19937 rng(
            static_cast<uint32_t>(getpid()) ^ (t * 7919U) ^
            static_cast<uint32_t>(
                std::chrono::steady_clock::now().time_since_epoch().count()));
        std::uniform_int_distribution<int> uniform_key(1, KEYSPACE);
        std::uniform_int_distribution<int> op_dist(1, 100);

        for (int i = 0;
             i < OPS_PER_CLIENT && keep_running.load(std::memory_order_relaxed);
             ++i) {
          int k;
          if (use_zipf) {
            k = static_cast<int>(zipf->next(rng));
          } else {
            k = uniform_key(rng);
          }
          k = std::clamp(k, 1, KEYSPACE);
          std::string key = "key_" + std::to_string(k);

          auto start = std::chrono::steady_clock::now();

          int roll = op_dist(rng);
          if (roll <= put_ratio) {
            if (use_curp) {
              client.put_curp(key, "v");
            } else {
              client.put(key, "v");
            }
          } else {
            client.get(key);
          }

          auto end = std::chrono::steady_clock::now();

          per_thread_lat[t].push_back(
              std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
                  end - start).count());
          total_ops.fetch_add(1, std::memory_order_relaxed);
        }

        fast_hits.fetch_add(client.fast_path_hits(), std::memory_order_relaxed);
      });
    }

    for (auto &th : threads) {
      th.join();
    }
    auto bench_end = std::chrono::steady_clock::now();

    cluster.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    double seconds = std::chrono::duration_cast<std::chrono::duration<double>>(
                         bench_end - bench_start).count();

    double throughput = 0.0;
    if (seconds > 0.0) {
      throughput = static_cast<double>(total_ops.load()) / seconds;
    }

    std::vector<double> all_lat;
    all_lat.reserve(static_cast<size_t>(clients) * OPS_PER_CLIENT);
    for (auto &v : per_thread_lat) {
      all_lat.insert(all_lat.end(), v.begin(), v.end());
    }

    double avg = 0.0;
    if (!all_lat.empty()) {
      avg = std::accumulate(all_lat.begin(), all_lat.end(), 0.0) /
            static_cast<double>(all_lat.size());
    }

    double p50 = percentile_ms(all_lat, 0.50);
    double p90 = percentile_ms(all_lat, 0.90);
    double p99 = percentile_ms(all_lat, 0.99);

    uint64_t total_writes = static_cast<uint64_t>(
        static_cast<double>(total_ops.load()) * put_ratio / 100.0);

    double hit_rate = 0.0;
    if (total_writes > 0) {
      hit_rate = 100.0 * static_cast<double>(fast_hits.load()) /
                 static_cast<double>(total_writes);
    }

    out << std::setw(13) << clients
        << std::fixed << std::setprecision(2)
        << std::setw(12) << avg
        << std::setw(13) << p50
        << std::setw(13) << p90
        << std::setw(13) << p99
        << std::setw(19) << std::setprecision(0) << throughput
        << std::setw(12) << std::setprecision(1) << hit_rate << "%\n";
    out.flush();

    std::cout << "clients=" << clients
              << " tput=" << std::setprecision(0) << throughput << " ops/s"
              << " fast-path=" << std::setprecision(1) << hit_rate << "%\n";
  }

  return 0;
}
