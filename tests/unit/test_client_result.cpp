/**
 * @file test_client_result.cpp
 * @brief Unit tests for ClientResult class
 */

#include "rollingraft/client.h"
#include "rollingraft/status.h"

#include <gtest/gtest.h>

using namespace rollingraft;

/**
 * ClientResult tests.
 *
 * These tests verify the Result<T, E> wrapper behavior.
 */

// ========== Construction Tests ==========

TEST(ClientResultTest, DefaultConstructor_IsError) {
  ClientResult result;
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.has_error());
}

TEST(ClientResultTest, SuccessConstructor) {
  ClientResult result("hello world");
  EXPECT_TRUE(result.ok());
  EXPECT_FALSE(result.has_error());
  EXPECT_EQ(result.value(), "hello world");
}

TEST(ClientResultTest, ErrorConstructor) {
  Status error = Status::Error("test error");
  ClientResult result(error);
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.has_error());
}

// ========== Value Access Tests ==========

TEST(ClientResultTest, Value_OnSuccess_ReturnsData) {
  ClientResult result("test data");
  EXPECT_EQ(result.value(), "test data");
}

TEST(ClientResultTest, Value_OnEmptySuccess_ReturnsEmptyString) {
  ClientResult result("");
  EXPECT_EQ(result.value(), "");
}

TEST(ClientResultTest, Error_OnSuccess_ReturnsOkStatus) {
  ClientResult result("success");
  const Status& status = result.error();
  EXPECT_TRUE(status.ok());
}

TEST(ClientResultTest, Error_OnError_ReturnsErrorStatus) {
  Status error = Status::NotLeader(1, "127.0.0.1:8001");
  ClientResult result(error);

  const Status& status = result.error();
  EXPECT_FALSE(status.ok());
  EXPECT_TRUE(status.IsNotLeader());
}

// ========== Error Message Tests ==========

TEST(ClientResultTest, ErrorMessage_OnSuccess_ReturnsEmpty) {
  ClientResult result("success");
  EXPECT_EQ(result.error_message(), "");
}

TEST(ClientResultTest, ErrorMessage_OnError_ReturnsString) {
  ClientResult result(Status::Error("something went wrong"));
  std::string msg = result.error_message();
  EXPECT_NE(msg.find("something went wrong"), std::string::npos);
}

// ========== Move Semantics Tests ==========

TEST(ClientResultTest, MoveConstructor_TransfersSuccess) {
  ClientResult original("move me");
  ClientResult moved(std::move(original));

  EXPECT_TRUE(moved.ok());
  EXPECT_EQ(moved.value(), "move me");
}

TEST(ClientResultTest, MoveConstructor_TransfersError) {
  ClientResult original(Status::Error("move error"));
  ClientResult moved(std::move(original));

  EXPECT_TRUE(moved.has_error());
  EXPECT_NE(moved.error_message().find("move error"), std::string::npos);
}

// ========== Edge Cases ==========

TEST(ClientResultTest, LargeValue_HandledCorrectly) {
  std::string large(1024 * 1024, 'x');  // 1MB string
  ClientResult result(large);

  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.value().size(), 1024 * 1024);
  EXPECT_EQ(result.value(), large);
}

TEST(ClientResultTest, SpecialCharacters_InValue) {
  std::string special = "Hello\nWorld\t!\r\n\\\"";
  ClientResult result(special);

  EXPECT_EQ(result.value(), special);
}

TEST(ClientResultTest, Unicode_InValue) {
  // UTF-8 bytes for "Hello 世界 🌍"
  std::string unicode = "Hello \xe4\xb8\x96\xe7\x95\x8c \xf0\x9f\x8c\x8d";
  ClientResult result(unicode);

  EXPECT_EQ(result.value(), unicode);
}
