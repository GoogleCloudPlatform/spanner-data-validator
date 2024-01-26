package com.google.migration.common;

import com.google.cloud.spanner.Dialect;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface DVTOptionsCore extends PipelineOptions {
  /**
   * The JDBC protocol (only postgresql and mysql are supported).
   */
  @Description("JDBC Protocol")
  @Default.String("postgresql")
  String getProtocol();

  void setProtocol(String value);

  /**
   * The JDBC server.
   */
  @Description("JDBC Server")
  @Default.String("localhost")
  String getServer();

  void setServer(String value);

  /**
   * The source DB.
   */
  @Description("Source DB")
  @Required
  String getSourceDB();

  void setSourceDB(String value);

  /**
   * The JDBC port
   */
  @Description("JDBC Port")
  @Default.Integer(5432)
  Integer getPort();

  void setPort(Integer value);

  /**
   * Username.
   */
  @Description("Username")
  @Required
  String getUsername();

  void setUsername(String value);

  /**
   * Password
   */
  @Description("Password")
  @Required
  String getPassword();

  void setPassword(String value);

  /**
   * ProjectId
   */
  @Description("ProjectId")
  @Required
  String getProjectId();

  void setProjectId(String value);

  /**
   * InstanceId
   */
  @Description("InstanceId")
  @Required
  String getInstanceId();

  void setInstanceId(String value);

  /**
   * Destination DatabaseId
   */
  @Description("Destination DatabaseId")
  @Required
  String getSpannerDatabaseId();

  void setSpannerDatabaseId(String value);

  @Description("Dialect of the Spanner database")
  @Default
  @Default.Enum("POSTGRESQL")
    // alternative: GOOGLE_STANDARD_SQL
  Dialect getDialect();

  void setDialect(Dialect dialect);

  /**
   * BQ Dataset
   */
  @Description("BQDatasetName")
  @Default.String("SpannerDVTDataset")
  String getBQDatasetName();

  void setBQDatasetName(String value);

  @Description("RunName")
  @Default.String("")
  String getRunName();

  void setRunName(String value);

  /**
   * BQ Table
   */
  @Description("BQTableName")
  @Default.String("SpannerDVTResults")
  String getBQTableName();

  void setBQTableName(String value);

  /**
   * Partition count
   */
  @Description("Partition count")
  @Default.Integer(100)
  Integer getPartitionCount();

  void setPartitionCount(Integer value);

  /**
   * Partition filter ratio
   */
  @Description("Partition filter ratio")
  @Default.Integer(-1)
  Integer getPartitionFilterRatio();

  void setPartitionFilterRatio(Integer value);

  /**
   * Table spec json
   */
  @Description("Table spec json")
  @Default.String("")
  String getTableSpecJson();

  void setTableSpecJson(String value);

  /**
   * Adjust timestamp precision
   */
  @Description("Adjust timestamp precision")
  @Default.Boolean(true)
  Boolean getAdjustTimestampPrecision();

  void setAdjustTimestampPrecision(Boolean value);

  @Description("Shard spec json file")
  @Default.String("")
  String getShardSpecJson();

  void setShardSpecJson(String value);
} // class