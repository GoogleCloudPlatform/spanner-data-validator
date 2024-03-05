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

import java.math.BigDecimal;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.joda.time.DateTime;

@DefaultCoder(AvroCoder.class)
public class TableSpec {

  public static final String INT_FIELD_TYPE = "INTEGER";
  public static final String LONG_FIELD_TYPE = "LONG";
  public static final String UUID_FIELD_TYPE = "UUID";
  public static final String TIMESTAMP_FIELD_TYPE = "TIMESTAMP";
  private String tableName;
  private String sourceQuery;
  private Integer rangeFieldIndex;
  private String rangeFieldType;
  private String destQuery;
  private BigDecimal rangeCoverage;
  private String rangeStart;
  private String rangeEnd;
  private DateTime lastUpdatedTimeCutoff;
  private Integer lastUpdatedTimeFieldIndex;
  private Integer partitionCount = -1;
  private Integer partitionFilterRatio = -1;

  public TableSpec() {
  }

  public TableSpec(String tableNameIn,
      String sourceQueryIn,
      String destQueryIn,
      Integer rangeFieldIndexIn,
      BigDecimal rangeCoverageIn,
      String rangeFieldTypeIn,
      String rangeStartIn,
      String rangeEndIn) {
    tableName = tableNameIn;
    sourceQuery = sourceQueryIn;
    destQuery = destQueryIn;
    rangeFieldIndex = rangeFieldIndexIn;
    rangeCoverage = rangeCoverageIn;
    rangeFieldType = rangeFieldTypeIn;
    rangeStart = rangeStartIn;
    rangeEnd = rangeEndIn;
  }

  public TableSpec(String tableNameIn,
      String sourceQueryIn,
      String destQueryIn,
      Integer rangeFieldIndexIn,
      BigDecimal rangeCoverageIn,
      String rangeFieldTypeIn,
      String rangeStartIn,
      String rangeEndIn,
      Integer partitionCountIn) {
    tableName = tableNameIn;
    sourceQuery = sourceQueryIn;
    destQuery = destQueryIn;
    rangeFieldIndex = rangeFieldIndexIn;
    rangeCoverage = rangeCoverageIn;
    rangeFieldType = rangeFieldTypeIn;
    rangeStart = rangeStartIn;
    rangeEnd = rangeEndIn;
    partitionCount = partitionCountIn;
  }

  public TableSpec(String tableNameIn,
      String sourceQueryIn,
      String destQueryIn,
      Integer rangeFieldIndexIn,
      BigDecimal rangeCoverageIn,
      String rangeFieldTypeIn,
      String rangeStartIn,
      String rangeEndIn,
      Integer partitionCountIn,
      Integer partitionFilterRatioIn) {
    tableName = tableNameIn;
    sourceQuery = sourceQueryIn;
    destQuery = destQueryIn;
    rangeFieldIndex = rangeFieldIndexIn;
    rangeCoverage = rangeCoverageIn;
    rangeFieldType = rangeFieldTypeIn;
    rangeStart = rangeStartIn;
    rangeEnd = rangeEndIn;
    partitionCount = partitionCountIn;
    partitionFilterRatio = partitionFilterRatioIn;
  }

  public TableSpec(String tableNameIn,
      String sourceQueryIn,
      String destQueryIn,
      Integer rangeFieldIndexIn,
      BigDecimal rangeCoverageIn,
      String rangeFieldTypeIn,
      String rangeStartIn,
      String rangeEndIn,
      DateTime lastUpdatedTimeCutoffIn,
      Integer lastUpdatedTimeFieldIndexIn) {
    tableName = tableNameIn;
    sourceQuery = sourceQueryIn;
    destQuery = destQueryIn;
    rangeFieldIndex = rangeFieldIndexIn;
    rangeCoverage = rangeCoverageIn;
    rangeFieldType = rangeFieldTypeIn;
    rangeStart = rangeStartIn;
    rangeEnd = rangeEndIn;
    lastUpdatedTimeCutoff = lastUpdatedTimeCutoffIn;
    lastUpdatedTimeFieldIndex = lastUpdatedTimeFieldIndexIn;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getSourceQuery() {
    return sourceQuery;
  }

  public void setSourceQuery(String sourceQuery) {
    this.sourceQuery = sourceQuery;
  }

  public Integer getRangeFieldIndex() {
    return rangeFieldIndex;
  }

  public void setRangeFieldIndex(Integer rangeFieldIndex) {
    this.rangeFieldIndex = rangeFieldIndex;
  }

  public String getDestQuery() {
    return destQuery;
  }

  public void setDestQuery(String destQuery) {
    this.destQuery = destQuery;
  }

  public BigDecimal getRangeCoverage() {
    return rangeCoverage;
  }

  public void setRangeCoverage(BigDecimal rangeCoverage) {
    this.rangeCoverage = rangeCoverage;
  }

  public String getRangeFieldType() {
    return rangeFieldType;
  }

  public void setRangeFieldType(String rangeFieldType) {
    this.rangeFieldType = rangeFieldType;
  }

  public String getRangeStart() {
    return rangeStart;
  }

  public void setRangeStart(String rangeStart) {
    this.rangeStart = rangeStart;
  }

  public String getRangeEnd() {
    return rangeEnd;
  }

  public void setRangeEnd(String rangeEnd) {
    this.rangeEnd = rangeEnd;
  }

  public DateTime getLastUpdatedTimeCutoff() {
    return lastUpdatedTimeCutoff;
  }

  public void setLastUpdatedTimeCutoff(DateTime lastUpdatedTimeCutoff) {
    this.lastUpdatedTimeCutoff = lastUpdatedTimeCutoff;
  }

  public Integer getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(Integer partitionCount) {
    this.partitionCount = partitionCount;
  }

  public Integer getLastUpdatedTimeFieldIndex() {
    return lastUpdatedTimeFieldIndex;
  }

  public void setLastUpdatedTimeFieldIndex(Integer lastUpdatedTimeFieldIndex) {
    this.lastUpdatedTimeFieldIndex = lastUpdatedTimeFieldIndex;
  }

  public Integer getPartitionFilterRatio() {
    return partitionFilterRatio;
  }

  public void setPartitionFilterRatio(Integer partitionFilterRatio) {
    this.partitionFilterRatio = partitionFilterRatio;
  }
} // class TableSpec