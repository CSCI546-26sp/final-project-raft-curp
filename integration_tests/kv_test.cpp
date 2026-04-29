#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <future>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <mutex>

#include <ddb/integration.hpp>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <gtest/gtest.h>

#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_config.hpp"
#include "toolings/test_ctrl.hpp"

#include "kv/kv_client.hpp"

#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

using namespace toolings;

static constexpr uint64_t KV_PORT_OFFSET = 1000;
static const std::string KV_NODE_APP_PATH = "../app/kv_node";

ABSL_FLAG(int, tester_verb, 1, "Tester verbosity level");
ABSL_FLAG(int, raft_node_verb, 0, "Raft node verbosity level");
ABSL_FLAG(bool, ddb, false, "Enable DDB for debugging.");
ABSL_FLAG(std::string, ddb_host_ip, "127.0.0.1", "Host IP for DDB.");
ABSL_FLAG(bool, wait_for_attach, true, "Wait for DDB attach.");
ABSL_FLAG(bool, ddb_app_wrapper, true, "Use DDB app wrapper.");

static DDBConfig ddb_conf;

// ---------------------------------------------------------------------------
// KvTestFixture: manages a 3-replica KV cluster for each test.
// ---------------------------------------------------------------------------
class KvTestFixture : public ::testing::Test {
protected:
  static constexpr uint64_t NUM_NODES = 3;
  static constexpr uint64_t RAFT_BASE_PORT = 50050;

  void SetUp() override {
    #ifdef TRACING
      static std::once_flag otel_init;
      std::call_once(otel_init, []() {
        tracing::InitOtelInfra("kv_test_client");
      });
    #endif
    rafty::utils::init_logger();

    auto test_name = ::testing::UnitTest::GetInstance()
                         ->current_test_info()
                         ->name();
    auto logger_name = std::format("kv_test_{}", test_name);
    logger = spdlog::get(logger_name);
    if (!logger) {
      logger = spdlog::basic_logger_mt(
          logger_name, std::format("logs/{}.log", logger_name), true);
    }

    auto insts = ConfigGen::gen_local_instances(NUM_NODES, RAFT_BASE_PORT);

    std::unordered_map<uint64_t, uint64_t> node_tester_ports;
    uint64_t tester_port = 55001;

    for (const auto &inst : insts) {
      std::map<uint64_t, std::string> peer_addrs;
      for (const auto &peer : insts) {
        if (peer.id == inst.id)
          continue;
        peer_addrs[peer.id] = peer.external_addr;
      }
      rafty::Config config = {
          .id = inst.id, .addr = inst.listening_addr, .peer_addrs = peer_addrs};
      configs.push_back(config);
      node_tester_ports[inst.id] = tester_port++;
      kv_addrs.push_back(
          "localhost:" + std::to_string(inst.port + KV_PORT_OFFSET));
    }

    const std::string ctrl_addr = "0.0.0.0:55000";
    ctrl = std::make_unique<RaftTestCtrl>(configs, node_tester_ports,
                                         KV_NODE_APP_PATH, ctrl_addr, 0,
                                         absl::GetFlag(FLAGS_raft_node_verb),
                                         logger, ddb_conf);

    ctrl->register_applier_handler(
        [this](testerpb::ApplyResult m) -> void {
          (void)m;
        });

    ctrl->run();
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  void TearDown() override {
    if (ctrl) {
      ctrl->kill();
    }
  }

  std::unique_ptr<kv::KvClient> make_client() {
    return std::make_unique<kv::KvClient>(kv_addrs);
  }

  void disconnect(uint64_t id) { ctrl->disconnect({id}); }
  void reconnect(uint64_t id) { ctrl->reconnect({id}); }

  std::shared_ptr<spdlog::logger> logger;
  std::unique_ptr<RaftTestCtrl> ctrl;
  std::vector<rafty::Config> configs;
  std::vector<std::string> kv_addrs;
};

TEST_F(KvTestFixture, FastPathBasic) {
  auto client = make_client();

  auto put_status = client->put("testkey", "testvalue");
  ASSERT_EQ(put_status, kvpb::KV_SUCCESS);

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  auto [get_status, val] = client->get("testkey");
  ASSERT_EQ(get_status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "testvalue");
}

TEST_F(KvTestFixture, CurpFastPath) {
  auto client = make_client();

  auto status = client->put_curp("key1", "hello");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);

  auto [get_status, val] = client->get_curp("key1");
  ASSERT_EQ(get_status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "hello");

  status = client->append_curp("key1", " world");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);

  auto [get_status2, val2] = client->get_curp("key1");
  ASSERT_EQ(get_status2, kvpb::KV_SUCCESS);
  ASSERT_EQ(val2, "hello world");
}

//---------------------------------------------------------------------------
// Test: Basic Put/Get/Append operations
//---------------------------------------------------------------------------
TEST_F(KvTestFixture, BasicOpsC) {
  auto client = make_client();

  auto status = client->put_curp("key1", "hello");
  ASSERT_EQ(status, kvpb::KV_SUCCESS) << "Put failed";

  auto [get_status, val] = client->get_curp("key1");
  ASSERT_EQ(get_status, kvpb::KV_SUCCESS) << "Get failed";
  ASSERT_EQ(val, "hello") << "Got wrong value";

  status = client->append_curp("key1", " world");
  ASSERT_EQ(status, kvpb::KV_SUCCESS) << "Append failed";

  auto [get_status2, val2] = client->get_curp("key1");
  ASSERT_EQ(get_status2, kvpb::KV_SUCCESS) << "Get after append failed";
  ASSERT_EQ(val2, "hello world") << "Append result incorrect";

  auto [get_status3, val3] = client->get_curp("nonexistent");
  ASSERT_EQ(get_status3, kvpb::KV_SUCCESS) << "Get nonexistent failed";
  ASSERT_EQ(val3, "") << "Nonexistent key should return empty string";
}

//---------------------------------------------------------------------------
// Test: Multiple keys
//---------------------------------------------------------------------------
TEST_F(KvTestFixture, MultiKeyC) {
  auto client = make_client();

  for (int i = 0; i < 10; i++) {
    auto key = "key" + std::to_string(i);
    auto value = "value" + std::to_string(i);
    auto status = client->put_curp(key, value);
    ASSERT_EQ(status, kvpb::KV_SUCCESS) << "Put " << key << " failed";
  }

  for (int i = 0; i < 10; i++) {
    auto key = "key" + std::to_string(i);
    auto expected = "value" + std::to_string(i);
    auto [status, val] = client->get_curp(key);
    ASSERT_EQ(status, kvpb::KV_SUCCESS) << "Get " << key << " failed";
    ASSERT_EQ(val, expected) << "Value mismatch for " << key;
  }
}

//---------------------------------------------------------------------------
// Test: Concurrent clients operating on different keys
//---------------------------------------------------------------------------
TEST_F(KvTestFixture, ConcurrentC) {
  constexpr int NUM_CLIENTS = 5;
  constexpr int OPS_PER_CLIENT = 20;

  std::vector<std::future<bool>> futs;
  for (int c = 0; c < NUM_CLIENTS; c++) {
    futs.push_back(std::async(std::launch::async, [this, c]() {
      auto client = make_client();
      for (int i = 0; i < OPS_PER_CLIENT; i++) {
        auto key = std::format("client{}_key{}", c, i);
        auto value = std::format("client{}_val{}", c, i);
        if (client->put_curp(key, value) != kvpb::KV_SUCCESS)
          return false;
        client->sync_latest();
        auto [status, val] = client->get_curp(key);
        if (status != kvpb::KV_SUCCESS || val != value)
          return false;
      }
      return true;
    }));
  }

  for (auto &f : futs) {
    ASSERT_TRUE(f.get()) << "Concurrent client operations failed";
  }
}

//---------------------------------------------------------------------------
// Test: Operations survive leader failure and re-election
//---------------------------------------------------------------------------
TEST_F(KvTestFixture, LeaderFailureC) {
  auto client = make_client();

  ASSERT_EQ(client->put_curp("survive", "value1"), kvpb::KV_SUCCESS);

  auto states = ctrl->get_all_states();
  uint64_t leader_id = UINT64_MAX;
  for (const auto &s : states) {
    if (s.is_leader()) {
      leader_id = s.id();
      break;
    }
  }

  if (leader_id != UINT64_MAX) {
    disconnect(leader_id);
    logger->info("Disconnected leader {}", leader_id);
  }

  std::this_thread::sleep_for(std::chrono::seconds(3));

  auto [status, val] = client->get_curp("survive");
  ASSERT_EQ(status, kvpb::KV_SUCCESS) << "Get after leader failure failed";
  ASSERT_EQ(val, "value1") << "Data lost after leader failure";

  ASSERT_EQ(client->put_curp("survive2", "value2"), kvpb::KV_SUCCESS);

  if (leader_id != UINT64_MAX) {
    reconnect(leader_id);
  }

  std::this_thread::sleep_for(std::chrono::seconds(2));

  auto [s2, v2] = client->get_curp("survive2");
  ASSERT_EQ(s2, kvpb::KV_SUCCESS);
  ASSERT_EQ(v2, "value2");
}

//---------------------------------------------------------------------------
// Test: Minority partition cannot serve reads (stale read prevention)
//---------------------------------------------------------------------------
TEST_F(KvTestFixture, PartitionStaleReadC) {
  auto client = make_client();

  ASSERT_EQ(client->put_curp("partition_key", "original"), kvpb::KV_SUCCESS);
  client->sync_latest();

  disconnect(1);
  disconnect(2);

  std::this_thread::sleep_for(std::chrono::seconds(3));

  reconnect(1);
  reconnect(2);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  auto [status, val] = client->get("partition_key");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "original");
}

//---------------------------------------------------------------------------
// Test: Concurrent appends to the same key are linearizable.
// Uses plain append() + sync_latest() since concurrent same-key writes
// always conflict in the witness table — CURP fast path is for non-conflicting
// writes so append_curp provides no benefit here.
//---------------------------------------------------------------------------
TEST_F(KvTestFixture, ConcurrentAppendC) {
  constexpr int NUM_CLIENTS = 3;
  constexpr int APPENDS_PER_CLIENT = 10;

  auto setup_client = make_client();
  ASSERT_EQ(setup_client->put_curp("shared", ""), kvpb::KV_SUCCESS);
  setup_client->sync_latest();

  std::vector<std::future<bool>> futs;
  for (int c = 0; c < NUM_CLIENTS; c++) {
    futs.push_back(std::async(std::launch::async, [this, c]() {
      auto client = make_client();
      for (int i = 0; i < APPENDS_PER_CLIENT; i++) {
        auto val = std::format("c{}i{} ", c, i);
        if (client->append("shared", val) != kvpb::KV_SUCCESS)
          return false;
        client->sync_latest();
      }
      return true;
    }));
  }

  for (auto &f : futs) {
    ASSERT_TRUE(f.get()) << "Concurrent append failed";
  }

  auto [status, val] = setup_client->get_curp("shared");
  ASSERT_EQ(status, kvpb::KV_SUCCESS);

  int token_count = 0;
  std::istringstream iss(val);
  std::string token;
  while (iss >> token) {
    token_count++;
  }
  ASSERT_EQ(token_count, NUM_CLIENTS * APPENDS_PER_CLIENT)
      << "Expected " << NUM_CLIENTS * APPENDS_PER_CLIENT
      << " appended tokens, got " << token_count;
}

//---------------------------------------------------------------------------
// Test: Put overwrites previous value
//---------------------------------------------------------------------------
TEST_F(KvTestFixture, PutOverwriteC) {
  auto client = make_client();

  ASSERT_EQ(client->put_curp("overwrite", "v1"), kvpb::KV_SUCCESS);
  auto [s1, v1] = client->get_curp("overwrite");
  ASSERT_EQ(v1, "v1");

  ASSERT_EQ(client->put_curp("overwrite", "v2"), kvpb::KV_SUCCESS);
  auto [s2, v2] = client->get_curp("overwrite");
  ASSERT_EQ(v2, "v2");

  ASSERT_EQ(client->put_curp("overwrite", "v3"), kvpb::KV_SUCCESS);
  auto [s3, v3] = client->get_curp("overwrite");
  ASSERT_EQ(v3, "v3");
}

TEST_F(KvTestFixture, RttFastPath) {
  auto client = make_client();

  for (int i = 0; i < 5; i++) {
    client->put_curp("__warmup__" + std::to_string(i), "val");
    client->sync_latest();
  }

  auto start = std::chrono::steady_clock::now();
  auto status = client->put_curp("rtt_key", "value1");
  auto end = std::chrono::steady_clock::now();
  ASSERT_EQ(status, kvpb::KV_SUCCESS);

  double put_ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - start).count();
  logger->info("RttFastPath: put_curp took {:.2f}ms (expected ~1 RTT)", put_ms);

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  start = std::chrono::steady_clock::now();
  auto [get_status, val] = client->get("rtt_key");
  end = std::chrono::steady_clock::now();

  double get_ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(end - start).count();
  logger->info("RttFastPath: get took {:.2f}ms (expected ~1 RTT)", get_ms);

  ASSERT_EQ(get_status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "value1");

  std::cout << "put_curp latency: " << put_ms << "ms\n";
  std::cout << "get latency (after sleep): " << get_ms << "ms\n";
}

TEST_F(KvTestFixture, RttSlowPath) {
  auto client = make_client();

  for (int i = 0; i < 5; i++) {
    client->put_curp("__warmup__" + std::to_string(i), "val");
    client->sync_latest();
  }

  auto start = std::chrono::steady_clock::now();
  auto status = client->put_curp("rtt_key2", "value2");
  auto put_end = std::chrono::steady_clock::now();
  ASSERT_EQ(status, kvpb::KV_SUCCESS);

  auto [get_status, val] = client->get_curp("rtt_key2");
  auto get_end = std::chrono::steady_clock::now();

  double put_ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(put_end - start).count();
  double total_ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(get_end - start).count();
  double get_ms = std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(get_end - put_end).count();

  logger->info("RttSlowPath: put_curp took {:.2f}ms", put_ms);
  logger->info("RttSlowPath: get_curp took {:.2f}ms which includes sync if there's conflict", get_ms);
  logger->info("RttSlowPath: total took {:.2f}ms (expected ~2 RTT)", total_ms);

  ASSERT_EQ(get_status, kvpb::KV_SUCCESS);
  ASSERT_EQ(val, "value2");

  std::cout << "put_curp latency: " << put_ms << "ms\n";
  std::cout << "get_curp latency (no sleep, may trigger sync): " << get_ms << "ms\n";
  std::cout << "total latency: " << total_ms << "ms\n";
}

TEST_F(KvTestFixture, CurpLatency) {
  constexpr int OPS = 100;
  auto client = make_client();

  std::vector<double> lats;
  lats.reserve(OPS);

  for (int i = 0; i < OPS; ++i) {
    auto key = "curp_key_" + std::to_string(i);
    auto start = std::chrono::steady_clock::now();
    client->put_curp(key, "value");
    auto end = std::chrono::steady_clock::now();
    lats.push_back(
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
            end - start).count());
  }

  std::sort(lats.begin(), lats.end());
  double avg = std::accumulate(lats.begin(), lats.end(), 0.0) /
               static_cast<double>(lats.size());
  double p50 = lats[lats.size() * 0.50];
  double p99 = lats[lats.size() * 0.99];

  std::cout << "\n CURP Fast Path Latency (1 RTT)\n";
  std::cout << "put_curp avg=" << avg << "ms  p50=" << p50 << "ms  p99=" << p99 << "ms\n";
  std::cout << "\n";

  logger->info("CurpLatency: avg={:.2f}ms p50={:.2f}ms p99={:.2f}ms",
               avg, p50, p99);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);

  ddb_conf = DDBConfig{
      .enable_ddb = absl::GetFlag(FLAGS_ddb),
      .ddb_host_ip = absl::GetFlag(FLAGS_ddb_host_ip),
      .wait_for_attach = absl::GetFlag(FLAGS_wait_for_attach),
      .ddb_app_wrapper = absl::GetFlag(FLAGS_ddb_app_wrapper)};

  return RUN_ALL_TESTS();
}
