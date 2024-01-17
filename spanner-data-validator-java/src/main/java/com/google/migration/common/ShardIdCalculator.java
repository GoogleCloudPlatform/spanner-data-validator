package com.google.migration.common;

public interface ShardIdCalculator {
  public Integer getShardIndexForShardKey(String shardKey, int numShards);
}