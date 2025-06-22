/*
 Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.google.migration.dto;

import com.google.cloud.spanner.Struct;
import com.google.migration.common.ShardIdCalculator;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DefaultCoder(AvroCoder.class)
public class ShardedHashResult extends HashResult {
  private static final Logger LOG = LoggerFactory.getLogger(ShardedHashResult.class);

  public Integer shardId;

  public ShardedHashResult() {
  }

  public ShardedHashResult(String keyIn,
      Boolean isSourceIn,
      String origValueIn,
      String sha256In,
      Integer shardIdIn,
      Long timestampThresholdValueIn) {
    super(keyIn, isSourceIn, origValueIn, sha256In, timestampThresholdValueIn);
    shardId = shardIdIn;
  }

  public ShardedHashResult(HashResult hashResult, Integer shardIdIn, Long timestampThresholdValueIn) {
    super(hashResult.key, hashResult.isSource, hashResult.origValue, hashResult.sha256, timestampThresholdValueIn);
    shardId = shardIdIn;
  }

  public static ShardedHashResult fromSpannerStruct(Struct spannerStruct,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision,
      int numShards,
      ShardIdCalculator shardIdCalculator,
      Long timestampThresholdValueIn,
      String tableName) {
    HashResult hashResult = HashResult.fromSpannerStruct(spannerStruct,
        keyIndex,
        rangeFieldType,
        adjustTimestampPrecision,
        -1,
        1L,
        "",
        "",
        "",
        false);
    Integer shardId = shardIdCalculator.getShardIndexForShardKey(hashResult.key, numShards);
    return new ShardedHashResult(hashResult, shardId, timestampThresholdValueIn);
  }

  public static ShardedHashResult fromJDBCResultSet(ResultSet resultSet,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision,
      Integer shardId,
      Long timestampThresholdValueIn)
      throws SQLException {
    HashResult hashResult = HashResult.fromJDBCResultSet(resultSet,
        keyIndex,
        rangeFieldType,
        adjustTimestampPrecision,
        -1);
    return new ShardedHashResult(hashResult, shardId, timestampThresholdValueIn);
  }
} // class ShardedHashResult