#include <gtest/gtest.h>

#include "rafty/raft.hpp"
#include "toolings/msg_queue.hpp"

using toolings::MessageQueue;
using rafty::ApplyResult;
using rafty::Config;
using rafty::Raft;

static Raft make_single_node_raft(MessageQueue<ApplyResult> &ready) {
  Config cfg;
  cfg.id = 1;
  cfg.addr = "127.0.0.1:0";
  cfg.peer_addrs = {};
  return Raft(cfg, ready);
}

TEST(WitnessTableTest, ConflictingWritesSameKeyReject) {
  MessageQueue<ApplyResult> ready(10);
  auto raft = make_single_node_raft(ready);

  auto r1 = raft.witness_record("PUT", "k", "v1", 1, 1);
  EXPECT_FALSE(r1.conflict);

  auto r2 = raft.witness_record("PUT", "k", "v2", 2, 1);
  EXPECT_TRUE(r2.conflict);
}

TEST(WitnessTableTest, DisjointKeysAccept) {
  MessageQueue<ApplyResult> ready(10);
  auto raft = make_single_node_raft(ready);

  auto r1 = raft.witness_record("PUT", "k1", "v1", 1, 1);
  auto r2 = raft.witness_record("PUT", "k2", "v2", 2, 1);
  EXPECT_FALSE(r1.conflict);
  EXPECT_FALSE(r2.conflict);
}

TEST(WitnessTableTest, GCRemovesEntries) {
  MessageQueue<ApplyResult> ready(10);
  auto raft = make_single_node_raft(ready);

  auto r1 = raft.witness_record("PUT", "k", "v1", 1, 1);
  ASSERT_FALSE(r1.conflict);

  // Conflict prior to GC.
  EXPECT_TRUE(raft.witness_record("PUT", "k", "v2", 2, 1).conflict);

  // After GC up to r1 index, write should be accepted again.
  raft.witness_gc(r1.witness_idx);
  EXPECT_FALSE(raft.witness_record("PUT", "k", "v3", 3, 1).conflict);
}

TEST(WitnessTableTest, GetRecoveryDataReturnsRecordedOps) {
  MessageQueue<ApplyResult> ready(10);
  auto raft = make_single_node_raft(ready);

  (void)raft.witness_record("PUT", "a", "1", 10, 1);
  (void)raft.witness_record("PUT", "b", "2", 11, 1);

  auto ops = raft.witness_get_recovery_data();
  EXPECT_EQ(ops.size(), 2u);

  // We don't rely on ordering (unordered_map); just verify set membership.
  bool saw_a = false;
  bool saw_b = false;
  for (const auto &op : ops) {
    if (op.key == "a") {
      saw_a = true;
      EXPECT_EQ(op.value, "1");
      EXPECT_EQ(op.op_type, "PUT");
      EXPECT_EQ(op.client_id, 10u);
      EXPECT_EQ(op.seq_num, 1u);
    } else if (op.key == "b") {
      saw_b = true;
      EXPECT_EQ(op.value, "2");
      EXPECT_EQ(op.op_type, "PUT");
      EXPECT_EQ(op.client_id, 11u);
      EXPECT_EQ(op.seq_num, 1u);
    }
  }
  EXPECT_TRUE(saw_a);
  EXPECT_TRUE(saw_b);
}

