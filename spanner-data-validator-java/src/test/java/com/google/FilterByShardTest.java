package com.google;

import com.google.cloud.spanner.Struct;
import com.google.migration.Helpers;
import com.google.migration.common.FilteryByShard;
import com.google.migration.dto.HashResult;
import java.util.List;
import org.junit.Test;

public class FilterByShardTest {
  @Test
  public void filterByShardBasicTest() {

    FilteryByShard fbs1 = new FilteryByShard(72L,
        "gdb",
        "nodes",
        "ddrkey",
        true);
    Struct spannerStruct = Struct.newBuilder()
        .set("type").to("eadp:account/personaId")
        .set("name").to("FFA24CRS-FUT;1100231409168")
        .set("from").to("eadp:account/nucleusId?1100162790004?NONE")
        .build();

    HashResult hr = new HashResult();
    fbs1.setLogicalShardId(hr, spannerStruct);
    System.out.println(hr.getLogicalShardId());

    fbs1.setTableName("edges");
    fbs1.setLogicalShardId(hr, spannerStruct);
    System.out.println(hr.getLogicalShardId());

    spannerStruct = Struct.newBuilder()
        .set("ddrkey").to(-443912000980910080L)
        .build();
    fbs1 = new FilteryByShard(2L,
        "tradehouse",
        "th_packagecontents",
        "ddrkey",
        true);
    fbs1.setLogicalShardId(hr, spannerStruct);
    Long logicalShardId = Long.parseLong(hr.getLogicalShardId());
    assert logicalShardId == 1L : "Expected shard ID to be 1, but got " + logicalShardId;

    spannerStruct = Struct.newBuilder()
        .set("ddrkey").to(8699761569824768L)
        .build();
    fbs1 = new FilteryByShard(2L,
        "tradehousei",
        "th_search_playercards",
        "ddrkey",
        true);
    fbs1.setLogicalShardId(hr, spannerStruct);
    logicalShardId = Long.parseLong(hr.getLogicalShardId());
    assert logicalShardId == 0L : "Expected shard ID to be 1, but got " + logicalShardId;
  }

  @Test
  public void commaSeparatedShardListTest() {
    String commaSeparatedShardList = "1,2,3,4, 5";
    List<String> shardList = Helpers.getShardListFromCommaSeparatedString(commaSeparatedShardList);
    assert shardList.size() == 5 : "Expected shard list to be of size 5, but got " +shardList.size();
    assert shardList.get(0).equals("1") : "Expected first item in shard list to be 1 but got " + shardList.get(0);
    assert shardList.get(4).equals("5") : "Expected first item in shard list to be 5 but got " + shardList.get(4);
    assert shardList.contains("4") : "Expected shard list to contain 4 but it does not";
  }
} // class FilterByShardTest
