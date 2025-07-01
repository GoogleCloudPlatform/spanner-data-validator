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

import com.google.migration.common.DVTOptionsCore;
import com.google.migration.dto.GCSObject;
import com.google.migration.dto.PartitionKey;
import com.google.migration.dto.TableSpec;
import com.google.migration.dto.session.ColumnPK;
import com.google.migration.dto.session.Index;
import com.google.migration.dto.session.IndexKey;
import com.google.migration.dto.session.Schema;
import com.google.migration.dto.session.SessionFileReader;
import com.google.migration.dto.session.SourceTable;
import com.google.migration.dto.session.SpannerTable;
import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Arrays;
import java.util.Optional;
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

  public static void toJsonFile(List<TableSpec> tableSpecs, String jsonFile) {
    try {
      JSONArray jsonarray = new JSONArray();

      for (TableSpec tableSpec : tableSpecs) {
        JSONObject jsonObject = new JSONObject();

        jsonObject.put("tableName", tableSpec.getTableName());
        jsonObject.put("sourceQuery", tableSpec.getSourceQuery());
        jsonObject.put("destQuery", tableSpec.getDestQuery());
        jsonObject.put("rangeFieldIndex", tableSpec.getRangeFieldIndex());
        jsonObject.put("rangeFieldType", tableSpec.getRangeFieldType());
        jsonObject.put("rangeStart", tableSpec.getRangeStart());
        jsonObject.put("rangeEnd", tableSpec.getRangeEnd());
        jsonObject.put("rangeCoverage", tableSpec.getRangeCoverage());
        jsonObject.put("partitionCount", tableSpec.getPartitionCount());
        jsonObject.put("partitionFilterRatio", tableSpec.getPartitionFilterRatio());
        jsonObject.put("timestampThresholdColIndex", tableSpec.getTimestampThresholdColIndex());
        jsonObject.put("timestampThresholdDeltaInMins", tableSpec.getTimestampThresholdDeltaInMins());
        jsonObject.put("timestampThresholdZoneOffset", tableSpec.getTimestampThresholdZoneOffset());

        if(tableSpec.getTimestampThresholdValue() != 0) {
          LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(tableSpec.getTimestampThresholdValue()), ZoneOffset.UTC);
          jsonObject.put("timestampThresholdValue", dateTime.toString());
        }

        jsonarray.put(jsonObject);
      } // for

      FileUtils.writeStringToFile(new File(jsonFile), jsonarray.toString(2), StandardCharsets.UTF_8);
    } catch (Exception ex) {
      LOG.error("Exception while writing table specs to json file");
      LOG.error(ex.getMessage());
      LOG.error(ex.getStackTrace().toString());
    }
  }

  public static List<TableSpec> getFromSessionFile(DVTOptionsCore options) {
    Schema schema = SessionFileReader.read(options.getSessionFileJson());
    List<TableSpec> tableSpecList = new ArrayList<>();
    for (String tableId : schema.getSpSchema().keySet()) {
      TableSpec tableSpec = new TableSpec();
      //Fetch the source and spanner tables from session object
      SpannerTable spannerTable = schema.getSpSchema().get(tableId);
      SourceTable sourceTable = schema.getSrcSchema().get(tableId);
      tableSpec.setTableName(spannerTable.getName());
      tableSpec.setPartitionCount(options.getPartitionCount());
      tableSpec.setPartitionFilterRatio(options.getPartitionFilterRatio());
      tableSpec.setRangeFieldIndex(0);
      tableSpec.setRangeCoverage(BigDecimal.valueOf(1));
      PartitionKey partitionKey = determinePartitionKey(sourceTable, spannerTable);
      if (partitionKey == null) {
        throw new RuntimeException(String.format("Unable to determine partition key for sourceTable: %s. ABORTING!!!", sourceTable));
      }
      tableSpec.setRangeStart(partitionKey.getPartitionKeyMinValue());
      tableSpec.setRangeEnd(partitionKey.getPartitionKeyMaxValue());
      tableSpec.setDestQuery(spannerTable.getSpannerQuery(partitionKey.getPartitionKeyColId(),
          sourceTable.getColIds(),
          !Helpers.isNullOrEmpty(options.getTransformationJarPath()) && !Helpers.isNullOrEmpty(options.getTransformationClassName()),
          options.getIncludeBackTicksInColNameForTableSpecGen()));
      tableSpec.setSourceQuery(sourceTable.getSourceQuery(partitionKey.getPartitionKeyColId(),
          spannerTable.getColIds(),
          options.getIncludeBackTicksInColNameForTableSpecGen()));
      tableSpec.setRangeFieldType(partitionKey.getPartitionKeyColDataType());
      tableSpec.setRangeFieldName(sourceTable.getColDefs().get(partitionKey.getPartitionKeyColId()).getName());
      tableSpecList.add(tableSpec);
    }
    return tableSpecList;
  }

  //Uses the PK as the partitionKey if it matches the criteria, otherwise looks for an
  //index on the same column.
  private static PartitionKey determinePartitionKey(SourceTable sourceTable, SpannerTable spannerTable) {
    PartitionKey partitionKey = null;

    //Store the column at the first ordinal position of the PK at Spanner
    ColumnPK[] spannerPKs = spannerTable.getPrimaryKeys();
    Arrays.sort(spannerPKs, Comparator.comparingInt(ColumnPK::getOrder));
    ColumnPK spannerPKAtFirstPosition = spannerPKs[0];


    //Get the ID and data type of the column in the first ordinal position of the source PK
    ColumnPK[] sourcePKs = sourceTable.getPrimaryKeys();
    if (sourcePKs.length == 0) {
      LOG.info("Source table does not have a PK, skipping validation");
      return  null;
    }
    Arrays.sort(sourcePKs, Comparator.comparingInt(ColumnPK::getOrder));
    ColumnPK sourcePKAtFirstPosition = sourcePKs[0];
    String sourcePKAtFirstPositionDataType = sourceTable.getColDefs().get(sourcePKAtFirstPosition.getColId()).getType().getName();

    //Check if source and spanner PK can be used to find PK.
    if (sourcePKAtFirstPosition.getColId().equals(spannerPKAtFirstPosition.getColId())) {
      partitionKey = createPartitionKey(sourcePKAtFirstPosition.getColId(), sourcePKAtFirstPositionDataType);
    }
    //No match with Spanner PK, check against indexes on spanner table
    if (partitionKey == null) {
      LOG.info("###########(Source PK, Spanner PK) -> No partition key found###########");
      partitionKey = searchSpannerIndexesForPartitionKey(sourcePKAtFirstPosition.getColId(), sourcePKAtFirstPositionDataType,
          spannerTable.getIndexes());
      }

    //source PK did not match with any PK or index at Spanner, check if any source indexes
    //can be used as a partitioning key
    if (partitionKey == null) {
      LOG.info("###########(Source PK, Spanner Indexes) -> No partition key found###########");
      Index[] sourceIndexes = sourceTable.getIndexes();
      if (sourceIndexes != null) {
        for (Index sourceIndex: sourceIndexes) {
          Arrays.sort(sourceIndex.getKeys(), Comparator.comparingInt(IndexKey::getOrder));
          IndexKey sourceIndexAtFirstPosition = sourceIndex.getKeys()[0];
          String sourceIndexAtFirstPositionDataType = sourceTable.getColDefs()
              .get(sourceIndexAtFirstPosition.getColId()).getType().getName();
          //first check with spanner PK
          if (sourceIndexAtFirstPosition.getColId().equals(spannerPKAtFirstPosition.getColId())) {
            partitionKey = createPartitionKey(sourceIndexAtFirstPosition.getColId(),
                sourceIndexAtFirstPositionDataType);
          }
          //if not found, check spanner indexes
          if (partitionKey == null) {
            LOG.info("###########(Source Indexes, Spanner PK) -> No partition key found###########");
            partitionKey = searchSpannerIndexesForPartitionKey(sourceIndexAtFirstPosition.getColId(),
                sourceIndexAtFirstPositionDataType, spannerTable.getIndexes());
          }
        }
      }

    }
    if (partitionKey == null) {
      LOG.info("###########(Source Indexes, Spanner Indexes) -> No partition key found###########");
      LOG.info("No suitable partitioning key was found for Source table: {}, Spanner table: {}", sourceTable.getName(), spannerTable.getName());
      LOG.info("Only (int, bigint) partition keys are supported, with a the following condition: There should exist an index (PK or secondary index) whose first ordinal position is a supported partitioning data type and is identical between source and spanner");
    }
    return partitionKey;
  }

  private static PartitionKey searchSpannerIndexesForPartitionKey(String sourceColId, String sourceColDataType, Index[] spannerIndexes) {
    if (spannerIndexes != null) {
      for (Index spannerIndex : spannerIndexes) {
        Arrays.sort(spannerIndex.getKeys(), Comparator.comparingInt(IndexKey::getOrder));
        IndexKey spannerIndexAtFirstPosition = spannerIndex.getKeys()[0];
        if (sourceColId.equals(spannerIndexAtFirstPosition.getColId())) {
          return createPartitionKey(sourceColId, sourceColDataType);
        }
      }
    }
    return null;
  }

  private static PartitionKey createPartitionKey(String colId, String colDataType) {
    switch (colDataType.toUpperCase()) {
      case "INT":
        return new PartitionKey(colId, "INTEGER", String.valueOf(Integer.MIN_VALUE), String.valueOf(Integer.MAX_VALUE));
      case "BIGINT":
        return new PartitionKey(colId, "LONG", String.valueOf(Long.MIN_VALUE), String.valueOf(Long.MAX_VALUE));
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
