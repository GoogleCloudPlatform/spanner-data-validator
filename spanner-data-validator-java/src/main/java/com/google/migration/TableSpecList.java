package com.google.migration;

import com.google.migration.common.DVTOptionsCore;
import com.google.migration.dto.ShardSpec;
import com.google.migration.dto.TableSpec;
import java.util.ArrayList;
import java.util.List;

public class TableSpecList {
  public static List<TableSpec> getTableSpecs() {
    ArrayList<TableSpec> tableSpecs = new ArrayList<>();

    // Must use p1 and p2 here because that's what the query expression
    // binder expects downstream
    TableSpec spec = new TableSpec(
        "member_events_type_mapping",
        "select id, name, durationInDays from member_events_type_mapping where id >= ? and id < ?",
        "select id, name, durationInDays from member_events_type_mapping where id >= @p1 "
            + " and id < @p2",
        0,
        100,
        TableSpec.LONG_FIELD_TYPE,
        "0",
        String.valueOf(Long.MAX_VALUE)
    );
    tableSpecs.add(spec);

    spec = new TableSpec(
        "member_events",
        "select id, memberEventId, numericId, eventTypeId, eventCode, detail from member_events where id >= ? and id < ?",
        "select id, memberEventId, numericId, eventTypeId, eventCode, detail from member_events where id >= @p1 "
            + " and id < @p2",
        0,
        100,
        TableSpec.LONG_FIELD_TYPE,
        "0",
        String.valueOf(Long.MAX_VALUE)
    );
    tableSpecs.add(spec);

    return tableSpecs;
  }

  public static List<TableSpec> getPostgresTableSpecs() {
    ArrayList<TableSpec> tableSpecs = new ArrayList<>();

    TableSpec spec = new TableSpec(
        "DataProductMetadata",
        "select * from \"data-products\".data_product_metadata where data_product_id > uuid(?) and data_product_id <= uuid(?)",
        "SELECT key, value, data_product_id FROM data_product_metadata "
            + "WHERE data_product_id > $1 AND data_product_id <= $2", // Spangres
        2,
        2,
        TableSpec.UUID_FIELD_TYPE,
        "00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff"
    );
    tableSpecs.add(spec);

    spec = new TableSpec(
        "DataProductRecords",
        "select * from \"data-products\".data_product_records "
            + "where id > uuid(?) and id <= uuid(?)",
        "SELECT * FROM data_product_records "
            + "WHERE id > $1 AND id <= $2",
        0, // zero based index of column that is key (in this case, it's id)
        2, // integer percentage of rows per partition range - top 2 percent *within range*
        TableSpec.UUID_FIELD_TYPE,
        "00000000-0000-0000-0000-000000000000",
        //"02010000-0000-0000-ffff-ffffffffffff"
        "ffffffff-ffff-ffff-ffff-ffffffffffff"
    );
    tableSpecs.add(spec);

    return tableSpecs;
  }

  public static class ShardSpecList {
    public static List<ShardSpec> getShardSpecs(DVTOptionsCore options) {
      ArrayList<ShardSpec> shardSpecs = new ArrayList<>();

      String host = options.getServer();
      String user = options.getUsername();
      String pass = options.getPassword();
      String db = options.getSourceDB();

      ShardSpec spec = new ShardSpec(host,
          user,
          pass,
          String.format("%s%d", db, 1),
          String.format("id%d", 1),
          0);
      shardSpecs.add(spec);

      spec = new ShardSpec(host,
          user,
          pass,
          String.format("%s%d", db, 2),
          String.format("id%d", 2),
          1);
      shardSpecs.add(spec);

      return shardSpecs;
    }
  } // class
}
