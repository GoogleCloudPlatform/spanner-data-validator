package com.google.migration.ck;

import com.google.migration.common.ShardIdCalculator;

public class CKShardIdCalculator implements ShardIdCalculator {

  @Override
  public Integer getShardIndexForShardKey(String shardKey, int numShards) {
    return null;
  }
} // class CKShardIdCalculator