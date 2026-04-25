#include <chrono>
#include <cstdint>
#include <format>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"

#include "kv/kv_client.hpp"
#include "kv.grpc.pb.h"

using namespace toolings;

static constexpr uint64_t KV_PORT_OFFSET = 1000;
static const std::string KV_NODE_APP_PATH = "../app/kv_node";

TEST(CurpSmoke, WitnessRPCsAndPutCurp) {
  rafty::utils::init_logger();
  auto logger = spdlog::basic_logger_mt("curp_smoke", "logs/curp_smoke.log", true);

  // 5-node cluster (matches project plan fast-path superquorum = 4/5).
  static constexpr uint64_t NUM_NODES = 5;
  static constexpr uint64_t RAFT_BASE_PORT = 50150; // avoid conflicts

  auto insts = ConfigGen::gen_local_instances(NUM_NODES, RAFT_BASE_PORT);

  std::vector<rafty::Config> configs;
  std::unordered_map<uint64_t, uint64_t> node_tester_ports;
  std::vector<std::string> kv_addrs;

  uint64_t tester_port = 56001;
  for (const auto &inst : insts) {
    std::map<uint64_t, std::string> peer_addrs;
    for (const auto &peer : insts) {
      if (peer.id == inst.id)
        continue;
      peer_addrs[peer.id] = peer.external_addr;
    }
    configs.push_back({.id = inst.id, .addr = inst.listening_addr, .peer_addrs = peer_addrs});
    node_tester_ports[inst.id] = tester_port++;
    kv_addrs.push_back("127.0.0.1:" + std::to_string(inst.port + KV_PORT_OFFSET));
  }

  const std::string ctrl_addr = "0.0.0.0:56000";
  auto ctrl = std::make_unique<RaftTestCtrl>(configs, node_tester_ports, KV_NODE_APP_PATH, ctrl_addr,
                                             0, /* raft_node_verb */ 0, logger, /*ddb*/ std::nullopt);
  ctrl->register_applier_handler([](testerpb::ApplyResult) {});
  ctrl->run();
  std::this_thread::sleep_for(std::chrono::seconds(2));

  kv::KvClient client(kv_addrs);

  const std::string key = "k";
  const std::string val = "v1";

  // 1) Witness record superquorum should succeed on empty tables.
  const uint64_t client_id = client.client_id();
  ASSERT_TRUE(client.witness_superquorum_record("PUT", key, val, client_id, 1, 4));

  // 2) Witness recovery data should be non-empty on at least one node.
  {
    grpc::ChannelArguments args;
    auto channel = grpc::CreateCustomChannel(kv_addrs[0], grpc::InsecureChannelCredentials(), args);
    auto stub = kvpb::KvService::NewStub(channel);
    kvpb::WitnessGetRecoveryDataRequest req;
    kvpb::WitnessGetRecoveryDataReply rep;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1500));
    auto status = stub->WitnessGetRecoveryData(&ctx, req, &rep);
    ASSERT_TRUE(status.ok());
    ASSERT_GE(rep.ops_size(), 1);
  }

  // 3) put_curp should still succeed functionally.
  ASSERT_EQ(client.put_curp(key, val), kvpb::KV_SUCCESS);

  auto [gst, got] = client.get(key);
  ASSERT_EQ(gst, kvpb::KV_SUCCESS);
  ASSERT_EQ(got, val);

  ctrl->kill();
}

