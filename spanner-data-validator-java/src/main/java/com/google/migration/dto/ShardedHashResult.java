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
      Integer shardIdIn) {
    super(keyIn, isSourceIn, origValueIn, sha256In);
    shardId = shardIdIn;
  }

  public ShardedHashResult(HashResult hashResult, Integer shardIdIn) {
    super(hashResult.key, hashResult.isSource, hashResult.origValue, hashResult.sha256);
    shardId = shardIdIn;
  }

  public static ShardedHashResult fromSpannerStruct(Struct spannerStruct,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision,
      int numShards,
      ShardIdCalculator shardIdCalculator) {
    HashResult hashResult = HashResult.fromSpannerStruct(spannerStruct,
        keyIndex,
        rangeFieldType,
        adjustTimestampPrecision);
    Integer shardId = shardIdCalculator.getShardIndexForShardKey(hashResult.key, numShards);
    return new ShardedHashResult(hashResult, shardId);
  }

  public static ShardedHashResult fromJDBCResultSet(ResultSet resultSet,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision,
      Integer shardId)
      throws SQLException {
    HashResult hashResult = HashResult.fromJDBCResultSet(resultSet,
        keyIndex,
        rangeFieldType,
        adjustTimestampPrecision);
    return new ShardedHashResult(hashResult, shardId);
  }
} // class ShardedHashResult