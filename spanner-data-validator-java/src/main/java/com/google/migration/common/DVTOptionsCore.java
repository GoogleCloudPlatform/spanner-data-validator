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
  String getUsername();

  void setUsername(String value);

  /**
   * Password
   */
  @Description("Password")
  @Default.String("")
  String getPassword();

  void setPassword(String value);

  /**
   * ProjectIdForSecret
   */
  @Description("ProjectIdForSecret")
  @Default.String("")
  String getProjectIdForSecret();

  void setProjectIdForSecret(String value);

  /**
   * DBPassFromSecret
   */
  @Description("DBPassFromSecret")
  @Default.String("")
  String getDBPassFromSecret();

  void setDBPassFromSecret(String value);

  /**
   * DBPassVersionForSecret
   */
  @Description("DBPassVersionForSecret")
  @Default.String("latest")
  String getDBPassVersionForSecret();

  void setDBPassVersionForSecret(String value);

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
   * BQ Table holding comparison results
   */
  @Description("BQTableName")
  @Default.String("SpannerDVTResults")
  String getBQTableName();

  void setBQTableName(String value);

  /**
   * BQ Table holding conflicting records
   */
  @Description("BQConflictRecordsTable")
  @Default.String("")
  String getConflictingRecordsBQTableName();

  void setConflictingRecordsBQTableName(String value);

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

  /**
   * Zero date time
   */
  @Description("ZeroDateTimeBehavior")
  @Default.Boolean(true)
  Boolean getZeroDateTimeBehavior();

  void setZeroDateTimeBehavior(Boolean value);

  /**
   * Verbose logging
   */
  @Description("Verbose logging")
  @Default.Boolean(false)
  Boolean getVerboseLogging();

  void setVerboseLogging(Boolean value);

  @Description("Shard spec json file")
  @Default.String("")
  String getShardSpecJson();

  void setShardSpecJson(String value);

  @Description("Spanner database project Id")
  @Default.String("")
  String getSpannerProjectId();

  void setSpannerProjectId(String value);

  @Description("Spanner migration tool's session file")
  @Default.String("")
  String getSessionFileJson();

  void setSessionFileJson(String value);

  @Description("Only generate the tableSpec from the session file and not run the pipeline")
  @Default.Boolean(false)
  Boolean getGenerateTableSpec();

  void setGenerateTableSpec(Boolean value);

  @Description("Custom jar location in Cloud Storage")
  @Default.String("")
  String getTransformationJarPath();

  void setTransformationJarPath(String value);

  @Description("Fully qualified class name having the custom transformation logic.  It is a mandatory field in case transformationJarPath is specified")
  @Default.String("")
  String getTransformationClassName();

  void setTransformationClassName(String value);

  @Description("String containing any custom parameters to be passed to the custom transformation class.")
  @Default.String("")
  String getTransformationCustomParameters();

  void setTransformationCustomParameters(String value);

  @Description("URL to connect to the source database host. It is the GCS location to the shard configuration.")
  @Default.String("")
  String getSourceConfigURL();

  void setSourceConfigURL(String value);

  /**
   * Max tables in effect at one time
   */
  @Description("Max tables in effect at one time")
  @Default.Integer(10)
  Integer getMaxTablesInEffectAtOneTime();

  void setMaxTablesInEffectAtOneTime(Integer value);

  /**
   * Max JDBC connections per JVM
   */
  @Description("Max JDBC connections per JVM")
  @Default.Integer(160)
  Integer getMaxJDBCConnectionsPerJVM();

  void setMaxJDBCConnectionsPerJVM(Integer value);

  @Description("Read from Spanner w/ high priority CPU (default is medium priority)")
  @Default.Boolean(false)
  Boolean getReadFromSpannerWithHighPriorityCPU();

  void setReadFromSpannerWithHighPriorityCPU(Boolean value);

  @Description("Include backticks for col names in table spec gen")
  @Default.Boolean(false)
  Boolean getIncludeBackTicksInColNameForTableSpecGen();

  void setIncludeBackTicksInColNameForTableSpecGen(Boolean value);
} // class