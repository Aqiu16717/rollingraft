/**
 * @file test_event_bus.cpp
 * @brief Unit tests for the agent-friendly event notification system
 */

#include <gtest/gtest.h>

#include "rollingraft/event.h"

using namespace rollingraft;

TEST(EventBusTest, SubscribeAndPublishTypedEvent) {
  EventBus bus;
  int call_count = 0;
  NodeRoleChangedEvent received_event;

  auto id = bus.Subscribe<NodeRoleChangedEvent>(
      [&call_count, &received_event](const NodeRoleChangedEvent& e) {
        ++call_count;
        received_event = e;
      });

  NodeRoleChangedEvent event{.node_id = 1,
                             .old_role = FOLLOWER,
                             .new_role = LEADER,
                             .term = 42,
                             .timestamp = std::chrono::steady_clock::now()};
  bus.Publish(event);

  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(received_event.node_id, 1);
  EXPECT_EQ(received_event.old_role, FOLLOWER);
  EXPECT_EQ(received_event.new_role, LEADER);
  EXPECT_EQ(received_event.term, 42);

  bus.Unsubscribe(id);
}

TEST(EventBusTest, SubscribeAllReceivesAllEvents) {
  EventBus bus;
  int event_count = 0;

  auto id = bus.SubscribeAll(
      [&event_count](const RaftEvent& /*e*/) { ++event_count; });

  bus.Publish(NodeRoleChangedEvent{});
  bus.Publish(LeaderChangedEvent{});
  bus.Publish(MembershipChangedEvent{});

  EXPECT_EQ(event_count, 3);

  bus.Unsubscribe(id);
}

TEST(EventBusTest, TypedSubscriberDoesNotReceiveOtherTypes) {
  EventBus bus;
  int role_change_count = 0;
  int leader_change_count = 0;

  auto id1 = bus.Subscribe<NodeRoleChangedEvent>(
      [&role_change_count](const NodeRoleChangedEvent& /*e*/) {
        ++role_change_count;
      });

  auto id2 = bus.Subscribe<LeaderChangedEvent>(
      [&leader_change_count](const LeaderChangedEvent& /*e*/) {
        ++leader_change_count;
      });

  bus.Publish(NodeRoleChangedEvent{});
  bus.Publish(NodeRoleChangedEvent{});

  EXPECT_EQ(role_change_count, 2);
  EXPECT_EQ(leader_change_count, 0);

  bus.Unsubscribe(id1);
  bus.Unsubscribe(id2);
}

TEST(EventBusTest, UnsubscribeStopsDelivery) {
  EventBus bus;
  int call_count = 0;

  auto id = bus.Subscribe<NodeRoleChangedEvent>(
      [&call_count](const NodeRoleChangedEvent& /*e*/) { ++call_count; });

  bus.Publish(NodeRoleChangedEvent{});
  EXPECT_EQ(call_count, 1);

  bus.Unsubscribe(id);
  bus.Publish(NodeRoleChangedEvent{});
  EXPECT_EQ(call_count, 1);  // Should not increase
}

TEST(EventBusTest, SubscriptionCount) {
  EventBus bus;
  EXPECT_EQ(bus.SubscriptionCount(), 0);

  auto id1 = bus.Subscribe<NodeRoleChangedEvent>([](const NodeRoleChangedEvent& /*e*/) {});
  EXPECT_EQ(bus.SubscriptionCount(), 1);

  auto id2 = bus.SubscribeAll([](const RaftEvent& /*e*/) {});
  EXPECT_EQ(bus.SubscriptionCount(), 2);

  bus.Unsubscribe(id1);
  EXPECT_EQ(bus.SubscriptionCount(), 1);

  bus.Unsubscribe(id2);
  EXPECT_EQ(bus.SubscriptionCount(), 0);
}

TEST(EventBusTest, MultipleSubscribersReceiveEvent) {
  EventBus bus;
  int count1 = 0;
  int count2 = 0;

  auto id1 = bus.Subscribe<NodeRoleChangedEvent>(
      [&count1](const NodeRoleChangedEvent& /*e*/) { ++count1; });

  auto id2 = bus.Subscribe<NodeRoleChangedEvent>(
      [&count2](const NodeRoleChangedEvent& /*e*/) { ++count2; });

  bus.Publish(NodeRoleChangedEvent{});

  EXPECT_EQ(count1, 1);
  EXPECT_EQ(count2, 1);

  bus.Unsubscribe(id1);
  bus.Unsubscribe(id2);
}

TEST(EventBusTest, WildcardAndTypedBothReceive) {
  EventBus bus;
  int typed_count = 0;
  int wildcard_count = 0;

  auto id1 = bus.Subscribe<NodeRoleChangedEvent>(
      [&typed_count](const NodeRoleChangedEvent& /*e*/) { ++typed_count; });

  auto id2 = bus.SubscribeAll(
      [&wildcard_count](const RaftEvent& /*e*/) { ++wildcard_count; });

  bus.Publish(NodeRoleChangedEvent{});

  EXPECT_EQ(typed_count, 1);
  EXPECT_EQ(wildcard_count, 1);

  bus.Unsubscribe(id1);
  bus.Unsubscribe(id2);
}

TEST(RaftEventTest, TypeCheckAndAccess) {
  NodeRoleChangedEvent event{.node_id = 42,
                             .old_role = FOLLOWER,
                             .new_role = CANDIDATE,
                             .term = 7,
                             .timestamp = std::chrono::steady_clock::now()};

  RaftEvent wrapped(event);

  EXPECT_TRUE(wrapped.Is<NodeRoleChangedEvent>());
  EXPECT_FALSE(wrapped.Is<LeaderChangedEvent>());
  EXPECT_STREQ(wrapped.Name(), "NodeRoleChanged");

  const auto& retrieved = wrapped.As<NodeRoleChangedEvent>();
  EXPECT_EQ(retrieved.node_id, 42);
  EXPECT_EQ(retrieved.new_role, CANDIDATE);
}
