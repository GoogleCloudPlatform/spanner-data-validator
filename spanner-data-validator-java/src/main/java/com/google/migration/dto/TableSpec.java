package com.google.migration.dto;

import java.io.IOException;
import java.io.InputStream;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(AvroCoder.class)
public class TableSpec {

  public static final String INT_FIELD_TYPE = "Integer";
  public static final String LONG_FIELD_TYPE = "Long";
  public static final String UUID_FIELD_TYPE = "UUID";
  public static final String TIMESTAMP_FIELD_TYPE = "Timestamp";
  private String tableName;
  private String sourceQuery;
  private Integer rangeFieldIndex;
  private String rangeFieldType;
  private String destQuery;
  private Integer rangeCoverage;
  private String rangeStart;
  private String rangeEnd;

  public TableSpec() {
  }

  public TableSpec(String tableNameIn,
      String sourceQueryIn,
      String destQueryIn,
      Integer rangeFieldIndexIn,
      Integer rangeCoverageIn,
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

  public Integer getRangeCoverage() {
    return rangeCoverage;
  }

  public void setRangeCoverage(Integer rangeCoverage) {
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
} // class TableSpec