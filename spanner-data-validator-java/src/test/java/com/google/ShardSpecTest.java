package com.google;

import static org.junit.Assert.assertEquals;

import com.google.migration.ShardSpecList;
import com.google.migration.dto.ShardSpec;
import java.util.List;
import org.junit.Test;

public class ShardSpecTest {
  @Test
  public void shardSpecReadFromJsonTest() throws Exception {
    List<ShardSpec> sList =
        ShardSpecList.getShardSpecsFromJsonFile("json/shard-spec-sample-v1.json");

    assertEquals(16, sList.size());
    assertEquals("testhost-001", sList.get(0).getHost());
    assertEquals("test-0000", sList.get(0).getDb());
    assertEquals("test-0007", sList.get(7).getDb());

    assertEquals("testhost-002", sList.get(15).getHost());
    assertEquals("test-0000", sList.get(8).getDb());
    assertEquals("test-0007", sList.get(7).getDb());
  }

  @Test
  public void singleShardSpecTest() throws Exception {
    List<ShardSpec> sList =
        ShardSpecList.getShardSpecsFromJsonFile("json/shard-spec-single-shard-sample.json");

    assertEquals(1, sList.size());
    assertEquals("testhost-001", sList.get(0).getHost());
    assertEquals("test-0000", sList.get(0).getDb());
  }
} // class