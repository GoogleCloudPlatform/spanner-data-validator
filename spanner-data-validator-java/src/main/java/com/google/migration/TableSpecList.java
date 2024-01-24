package com.google.migration;

import com.google.gson.JsonObject;
import com.google.migration.common.DVTOptionsCore;
import com.google.migration.dto.ShardSpec;
import com.google.migration.dto.TableSpec;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSpecList {
  private static final Logger LOG = LoggerFactory.getLogger(TableSpecList.class);

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
        Constants.PERCENTAGE_CALCULATION_DENOMINATOR, // percentage of range we want to retrieve
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
        Constants.PERCENTAGE_CALCULATION_DENOMINATOR,
        TableSpec.LONG_FIELD_TYPE,
        "0",
        String.valueOf(Long.MAX_VALUE)
    );
    tableSpecs.add(spec);

    return tableSpecs;
  }

  public static List<TableSpec> getTableSpecsWithLastUpdatedTimeCutoff() {
    ArrayList<TableSpec> tableSpecs = new ArrayList<>();

    // don't want to filter out by last updated time in the query because it's not indexed
    TableSpec spec = new TableSpec(
        "member_events_type_mapping",
        "select id, name, durationInDays from member_events_type_mapping where id >= ? and id < ?",
        // Must use p1 and p2 here because that's what the query expression
        // binder expects downstream
        "select id, name, durationInDays from member_events_type_mapping where id >= @p1 "
            + " and id < @p2",
        0,
        Constants.PERCENTAGE_CALCULATION_DENOMINATOR,
        TableSpec.LONG_FIELD_TYPE,
        "0",
        String.valueOf(Long.MAX_VALUE),
        DateTime.now().minusHours(1),
        3
    );
    tableSpecs.add(spec);

    // don't want to filter out by last updated time in the query because it's not indexed
    spec = new TableSpec(
        "member_events",
        "select id, memberEventId, numericId, eventTypeId, eventCode, detail from member_events where id >= ? and id < ?",
        "select id, memberEventId, numericId, eventTypeId, eventCode, detail from member_events where id >= @p1 "
            + " and id < @p2",
        0,
        Constants.PERCENTAGE_CALCULATION_DENOMINATOR,
        TableSpec.LONG_FIELD_TYPE,
        "0",
        String.valueOf(Long.MAX_VALUE),
        DateTime.now().minusHours(1),
        7
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
        2/100 * Constants.PERCENTAGE_CALCULATION_DENOMINATOR,
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
        2/100 * Constants.PERCENTAGE_CALCULATION_DENOMINATOR, // integer percentage of rows per partition range - top 2 percent *within range*
        TableSpec.UUID_FIELD_TYPE,
        "00000000-0000-0000-0000-000000000000",
        //"02010000-0000-0000-ffff-ffffffffffff"
        "ffffffff-ffff-ffff-ffff-ffffffffffff"
    );
    tableSpecs.add(spec);

    return tableSpecs;
  }

  public static List<TableSpec> getFromJsonFile(String jsonFile) throws IOException {
    try {
      ClassLoader classloader = Thread.currentThread().getContextClassLoader();
      InputStream is = classloader.getResourceAsStream(jsonFile);

      String jsonStr = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      JSONArray jsonarray = new JSONArray(jsonStr);

      List<TableSpec> tableSpecs = new ArrayList<>();

      for (int i = 0; i < jsonarray.length(); i++) {
        TableSpec tableSpec = new TableSpec();

        JSONObject jsonObject = jsonarray.getJSONObject(i);
        tableSpec.setTableName(jsonObject.getString("tableName"));
        tableSpec.setSourceQuery(jsonObject.getString("sourceQuery"));
        tableSpec.setDestQuery(jsonObject.getString("destQuery"));
        tableSpec.setRangeFieldIndex(jsonObject.getInt("rangeFieldIndex"));
        tableSpec.setRangeFieldType(jsonObject.getString("rangeFieldType"));
        tableSpec.setRangeStart(jsonObject.getString("rangeStart"));
        tableSpec.setRangeEnd(jsonObject.getString("rangeEnd"));
        tableSpec.setPartitionCount(jsonObject.getInt("partitionCount"));
        tableSpec.setPartitionFilterRatio(jsonObject.getInt("partitionFilterRatio"));

        tableSpecs.add(tableSpec);

        return tableSpecs;
      } // for
    } catch (Exception ex) {
      LOG.error("Exception while loading table specs from json file");
      LOG.error(ex.getMessage());
      LOG.error(ex.getStackTrace().toString());
    }

    return null;
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
          String.format("%s", db),
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
