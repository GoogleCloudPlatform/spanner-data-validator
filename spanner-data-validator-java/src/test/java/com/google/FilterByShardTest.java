package com.google;

import com.google.cloud.spanner.Struct;
import com.google.migration.Helpers;
import com.google.migration.common.FilteryByShard;
import com.google.migration.dto.HashResult;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.commons.lang3.SerializationUtils;
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
    fbs1.setLogicalShardId(hr, spannerStruct, true);
    System.out.println(hr.getLogicalShardId());

    fbs1.setTableName("edges");
    fbs1.setLogicalShardId(hr, spannerStruct, true);
    System.out.println(hr.getLogicalShardId());

    spannerStruct = Struct.newBuilder()
        .set("ddrkey").to(-443912000980910080L)
        .build();
    fbs1 = new FilteryByShard(2L,
        "tradehouse",
        "th_packagecontents",
        "ddrkey",
        true);
    fbs1.setLogicalShardId(hr, spannerStruct, true);
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
    fbs1.setLogicalShardId(hr, spannerStruct, true);
    logicalShardId = Long.parseLong(hr.getLogicalShardId());
    assert logicalShardId == 0L : "Expected shard ID to be 1, but got " + logicalShardId;

    spannerStruct = Struct.newBuilder()
        .set("ddrkey").to(-9223354496199950336L)
        .build();
    fbs1 = new FilteryByShard(36L,
        "itemhouse",
        "ih_items",
        "ddrkey",
        true);
    fbs1.setLogicalShardId(hr, spannerStruct, true);
    logicalShardId = Long.parseLong(hr.getLogicalShardId());
    assert logicalShardId == 25L : "Expected shard ID to be 25, but got " + logicalShardId;
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

  @Test
  public void singleValueShardListTest() {
    String commaSeparatedShardList = "25";
    List<String> shardList = Helpers.getShardListFromCommaSeparatedString(commaSeparatedShardList);
    assert shardList.size() == 1 : "Expected shard list to be of size 1, but got " +shardList.size();
    assert shardList.get(0).equals("25") : "Expected first item in shard list to be 25 but got " + shardList.get(0);
  }

  @Test
  public void filterbyShardCountTest() {
    HashResult hr = new HashResult();
    Struct spannerStruct = Struct.newBuilder()
        .set("ddrkey").to(-9223354496199950336L)
        .build();
    FilteryByShard fbs1 = new FilteryByShard(36L,
        "itemhouse",
        "ih_items",
        "ddrkey",
        true);
    fbs1.setLogicalShardId(hr, spannerStruct, true);
    Long logicalShardId = Long.parseLong(hr.getLogicalShardId());
    assert logicalShardId == 25L : "Expected shard ID to be 25, but got " + logicalShardId;
  }

  @Test
  public void testSerialization() throws IOException {
    FilteryByShard fbs1 = new FilteryByShard(72L,
        "gdb",
        "nodes",
        "ddrkey",
        true);

    SerializableUtils.serializeToByteArray((Serializable) fbs1);
    // HashResult hr = new HashResult();
    // SerializationUtils.serialize(hr);

    // DatumWriter<FilteryByShard> datumWriter = new SpecificDatumWriter<>(FilteryByShard.class);
    // ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    // Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
    // datumWriter.write(fbs1, encoder);
    // encoder.flush();
    // outputStream.close();
  }
} // class FilterByShardTest
