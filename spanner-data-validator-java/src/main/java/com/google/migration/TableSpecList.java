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

package com.google.migration;

import com.google.migration.dto.GCSObject;
import com.google.migration.dto.TableSpec;
import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
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
        BigDecimal.ONE,
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
        BigDecimal.ONE,
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
        BigDecimal.ONE,
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
        BigDecimal.ONE,
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
        BigDecimal.ONE,
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
        BigDecimal.ONE,
        TableSpec.UUID_FIELD_TYPE,
        "00000000-0000-0000-0000-000000000000",
        //"02010000-0000-0000-ffff-ffffffffffff"
        "ffffffff-ffff-ffff-ffff-ffffffffffff"
    );
    tableSpecs.add(spec);

    return tableSpecs;
  }

  public static List<TableSpec> getFromJsonString(String jsonStr) {
    try {
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

        tableSpec.setRangeCoverage(
            jsonObject.isNull("rangeCoverage") ?
                BigDecimal.valueOf(100) :
                jsonObject.getBigDecimal("rangeCoverage"));

        tableSpec.setPartitionCount(
            jsonObject.isNull("partitionCount") ?
                100 :
                jsonObject.getInt("partitionCount"));

        tableSpec.setPartitionFilterRatio(
            jsonObject.isNull("partitionFilterRatio") ?
                -1 :
                jsonObject.getInt("partitionFilterRatio"));

        tableSpec.setTimestampThresholdColIndex(
            jsonObject.isNull("timestampThresholdColIndex") ?
                -1 :
                jsonObject.getInt("timestampThresholdColIndex"));

        tableSpec.setTimestampThresholdDeltaInMins(
            jsonObject.isNull("timestampThresholdDeltaInMins") ?
                0 :
                jsonObject.getInt("timestampThresholdDeltaInMins"));

        tableSpec.setTimestampThresholdZoneOffset(
            jsonObject.isNull("timestampThresholdZoneOffset") ?
                0 :
                jsonObject.getInt("timestampThresholdZoneOffset"));

        if(!jsonObject.isNull("timestampThresholdValue")) {
          String rawVal = jsonObject.getString("timestampThresholdValue");
          LocalDateTime dateTime = LocalDateTime.parse(rawVal);
          Instant instant = dateTime.toInstant(ZoneOffset.ofHours(tableSpec.getTimestampThresholdZoneOffset()));
          tableSpec.setTimestampThresholdValue(instant.toEpochMilli());
        } else {
          tableSpec.setTimestampThresholdValue(0L);
        }

        tableSpecs.add(tableSpec);
      } // for

      return tableSpecs;
    } catch (Exception ex) {
      LOG.error("Exception while loading table specs from json string");
      LOG.error(ex.getMessage());
      LOG.error(ex.getStackTrace().toString());
    }

    return null;
  }

  public static List<TableSpec> getFromJsonResource(String jsonFile) {
    try {
      ClassLoader classloader = Thread.currentThread().getContextClassLoader();
      InputStream is = classloader.getResourceAsStream(jsonFile);

      String jsonStr = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      return getFromJsonString(jsonStr);
    } catch (Exception ex) {
      LOG.error("Exception while loading table specs from json resource");
      LOG.error(ex.getMessage());
      LOG.error(ex.getStackTrace().toString());
    }

    return null;
  }

  public static List<TableSpec> getFromJsonFile(String projectId, String jsonFile) {
    String jsonStr = null;

    GCSObject gcsObject = Helpers.getGCSObjectFromFullPath(jsonFile);
    if(gcsObject != null) {
      jsonStr = Helpers.getFileFromGCS(projectId, gcsObject.getBucket(), gcsObject.getObjectName());
    }

    try {
      if(Helpers.isNullOrEmpty(jsonStr)) {
        jsonStr = FileUtils.readFileToString(new File(jsonFile), StandardCharsets.UTF_8);
      }

      return getFromJsonString(jsonStr);
    } catch (Exception ex) {
      LOG.error("Exception while loading table specs from json file");
      LOG.error(ex.getMessage());
      LOG.error(ex.getStackTrace().toString());
    }

    return null;
  }

  private static List<TableSpec> getFromJsonFileInGCS(String jsonFile) {
    try {
      String jsonStr = FileUtils.readFileToString(new File(jsonFile), StandardCharsets.UTF_8);
      return getFromJsonString(jsonStr);
    } catch (Exception ex) {
      LOG.error("Exception while loading table specs from json file");
      LOG.error(ex.getMessage());
      LOG.error(ex.getStackTrace().toString());
    }

    return null;
  }
} // class