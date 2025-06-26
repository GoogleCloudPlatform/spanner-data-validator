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

import static com.google.migration.SharedTags.jdbcTag;
import static com.google.migration.SharedTags.matchedRecordCountTag;
import static com.google.migration.SharedTags.matchedRecordsTag;
import static com.google.migration.SharedTags.sourceRecordCountTag;
import static com.google.migration.SharedTags.sourceRecordsTag;
import static com.google.migration.SharedTags.spannerTag;
import static com.google.migration.SharedTags.targetRecordCountTag;
import static com.google.migration.SharedTags.targetRecordsTag;
import static com.google.migration.SharedTags.unmatchedJDBCRecordCountTag;
import static com.google.migration.SharedTags.unmatchedJDBCRecordValuesTag;
import static com.google.migration.SharedTags.unmatchedJDBCRecordsTag;
import static com.google.migration.SharedTags.unmatchedSpannerRecordCountTag;
import static com.google.migration.SharedTags.unmatchedSpannerRecordValuesTag;
import static com.google.migration.SharedTags.unmatchedSpannerRecordsTag;
import static com.google.migration.TableSpecList.getTableSpecs;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.migration.common.DVTOptionsCore;
import com.google.migration.common.FilteryByShard;
import com.google.migration.common.HikariPoolableDataSourceProvider;
import com.google.migration.common.JDBCRowMapper;
import com.google.migration.common.SecretManagerAccessorImpl;
import com.google.migration.common.ShardFileReader;
import com.google.migration.dofns.CountMatchesDoFn;
import com.google.migration.dofns.CountMatchesWithShardFilteringDoFn;
import com.google.migration.dofns.CustomTransformationDoFn;
import com.google.migration.dofns.MapWithRangeFn;
import com.google.migration.dofns.MapWithRangeFn.MapWithRangeType;
import com.google.migration.dto.ComparerResult;
import com.google.migration.dto.HashResult;
import com.google.migration.dto.PartitionRange;
import com.google.migration.dto.Shard;
import com.google.migration.dto.SourceRecord;
import com.google.migration.dto.TableSpec;
import com.google.migration.dto.session.Schema;
import com.google.migration.dto.session.SessionFileReader;
import com.google.migration.partitioning.PartitionRangeListFetcher;
import com.google.migration.partitioning.PartitionRangeListFetcherFactory;
import com.google.migration.transform.CustomTransformation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.ReadAll;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.metrics.Gauge;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToSpannerDVTWithHash {
  protected static final String POSTGRES_JDBC_DRIVER = "org.postgresql.Driver";
  protected static final String MYSQL_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
  private static final Logger LOG = LoggerFactory.getLogger(JDBCToSpannerDVTWithHash.class);

  // [START JDBCToSpannerDVTWithHash_options]
  public interface JDBCToSpannerDVTWithHashOptions extends DVTOptionsCore {
  }
  // [END JDBCToSpannerDVTWithHash_options]

  protected static List<TableFieldSchema> getComparisonResultsBQWriteCols() {
    return Arrays.asList(
        new TableFieldSchema()
            .setName("run_name")
            .setType("STRING")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("table_or_query")
            .setType("STRING")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("range")
            .setType("STRING")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("source_count")
            .setType("INT64")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("target_count")
            .setType("INT64")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("match_count")
            .setType("INT64")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("source_conflict_count")
            .setType("INT64")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("target_conflict_count")
            .setType("INT64")
            .setMode("REQUIRED"));
  }

  protected static List<TableFieldSchema> getConflictingRecordsBQWriteCols() {
    return Arrays.asList(
        new TableFieldSchema()
            .setName("run_name")
            .setType("STRING")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("table_or_query")
            .setType("STRING")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("jdbc_or_spanner")
            .setType("STRING")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("range")
            .setType("STRING")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("key")
            .setType("STRING")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("hash")
            .setType("STRING")
            .setMode("REQUIRED"),
        new TableFieldSchema()
            .setName("orig_value")
            .setType("STRING")
            .setMode("NULLABLE"));
  }

  protected static BigQueryIO.Write<ComparerResult> getComparisonResultsBQWriter(DVTOptionsCore options,
      String tableName) {

    TableSchema bqSchema = new TableSchema().setFields(getComparisonResultsBQWriteCols());

    BigQueryIO.Write<ComparerResult> bqWrite = BigQueryIO.<ComparerResult>write()
        .to(String.format("%s:%s.%s",
            options.getProjectId(),
            options.getBQDatasetName(),
            options.getBQTableName()))
        .withFormatFunction(
            (ComparerResult x) -> new TableRow()
                .set("run_name", x.runName)
                .set("table_or_query", tableName)
                .set("range", x.range)
                .set("source_count", x.sourceCount)
                .set("target_count", x.targetCount)
                .set("match_count", x.matchCount)
                .set("source_conflict_count", x.sourceConflictCount)
                .set("target_conflict_count", x.targetConflictCount))
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withSchema(bqSchema)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .withMethod(Write.Method.STORAGE_WRITE_API);

    return bqWrite;
  }

  protected static BigQueryIO.Write<HashResult> getConflictingRecordsBQWriter(DVTOptionsCore options,
      String runName,
      String tableName,
      String jdbcOrSpanner) {

    TableSchema bqSchema = new TableSchema().setFields(getConflictingRecordsBQWriteCols());

    BigQueryIO.Write<HashResult> bqWrite = BigQueryIO.<HashResult>write()
        .to(String.format("%s:%s.%s",
            options.getProjectId(),
            options.getBQDatasetName(),
            options.getConflictingRecordsBQTableName()))
        .withFormatFunction(
            (HashResult x) -> new TableRow()
                .set("run_name", runName)
                .set("table_or_query", tableName)
                .set("jdbc_or_spanner", jdbcOrSpanner)
                .set("range", x.range)
                .set("key", x.key)
                .set("hash", x.sha256)
                .set("orig_value", x.origValue))
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withSchema(bqSchema)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .withMethod(Write.Method.STORAGE_WRITE_API);

    return bqWrite;
  }

  protected static void configureComparisonPipeline(Pipeline p,
      PipelineTracker pipelineTracker,
      DVTOptionsCore options,
      TableSpec tableSpec,
      BigQueryIO.Write<ComparerResult> comparerResultWrite,
      BigQueryIO.Write<HashResult> jdbcConflictingRecordsWriter,
      BigQueryIO.Write<HashResult> spannerConflictingRecordsWriter,
      CustomTransformation customTransformation,
      Schema schema) {

    Integer partitionCount = options.getPartitionCount();
    if(tableSpec.getPartitionCount() > 0) {
      partitionCount = tableSpec.getPartitionCount();
    }

    Integer partitionFilterRatio = options.getPartitionFilterRatio();
    if(tableSpec.getPartitionFilterRatio() > 0) {
      partitionFilterRatio = tableSpec.getPartitionFilterRatio();
    }

    List<PartitionRange> bRanges = getPartitionRanges(tableSpec,
        partitionCount,
        partitionFilterRatio);

    Helpers.printPartitionRanges(bRanges, tableSpec.getTableName());

    String tableName = tableSpec.getTableName();
    String shardConfigurationFileUrl = options.getSourceConfigURL();

    String createRangesForTableStep = String.format("CreateRangesForTable-%s", tableName);
    PCollection<PartitionRange> pRanges = p.apply(createRangesForTableStep, Create.of(bRanges));

    // get ranges of keys
    String partitionRangesViewStep = String.format("PartitionRangesForTable-%s", tableName);
    final PCollectionView<List<PartitionRange>> partitionRangesView =
        pRanges.apply(partitionRangesViewStep, View.asList());

    PCollection<HashResult> spannerRecords =
        getSpannerRecords(tableName,
            pipelineTracker,
            tableSpec.getDestQuery(),
            tableSpec.getRangeFieldIndex(),
            tableSpec.getRangeFieldType(),
            options,
            pRanges,
            tableSpec.getTimestampThresholdColIndex());

    pipelineTracker.addToSpannerReadList(spannerRecords);

    // Map Range [start, end) + hash => HashResult (spanner)
    String mapWithRangesForSpannerStep =
        String.format("MapWithRangesSpannerRecordsForTable-%s", tableName);
    PCollection<KV<String, HashResult>> mappedWithHashSpannerRecords =
        spannerRecords.apply(mapWithRangesForSpannerStep, ParDo.of(new MapWithRangeFn(partitionRangesView,
                MapWithRangeType.RANGE_PLUS_HASH,
                tableSpec.getRangeFieldType()))
            .withSideInputs(partitionRangesView));

    PCollection<HashResult> jdbcRecords;

    if(Helpers.isNullOrEmpty(shardConfigurationFileUrl)) {
      jdbcRecords =
          getJDBCRecords(tableName,
              pipelineTracker,
              tableSpec.getSourceQuery(),
              tableSpec.getRangeFieldIndex(),
              tableSpec.getRangeFieldType(),
              options,
              pRanges,
              tableSpec.getTimestampThresholdColIndex(),
              customTransformation,
              schema);
    } else {
      jdbcRecords =
          getJDBCRecordsWithSharding(tableName,
              pipelineTracker,
              tableSpec.getSourceQuery(),
              tableSpec.getRangeFieldIndex(),
              tableSpec.getRangeFieldType(),
              options,
              pRanges,
              tableSpec.getTimestampThresholdColIndex(),
              customTransformation,
              schema);
    }

    // Map Range [start, end) + hash => HashResult (JDBC)
    String mapWithRangesForJDBCStep =
        String.format("MapWithRangesJDBCRecordsForTable-%s", tableName);
    PCollection<KV<String, HashResult>> mappedWithHashJdbcRecords =
        jdbcRecords
            .apply(mapWithRangesForJDBCStep, ParDo.of(new MapWithRangeFn(partitionRangesView,
                MapWithRangeType.RANGE_PLUS_HASH,
                tableSpec.getRangeFieldType()))
            .withSideInputs(partitionRangesView));

    // Group by range [start, end) + hash => {JDBC HashResult if it exists, Spanner HashResult if it exists}
    String groupByKeyStep = String.format("GroupByKeyForTable-%s", tableName);
    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(jdbcTag, mappedWithHashJdbcRecords)
            .and(spannerTag, mappedWithHashSpannerRecords)
            .apply(groupByKeyStep, CoGroupByKey.create());

    DoFn<KV<String, CoGbkResult>, KV<String, Long>> countMatchesDoFn;

    String stepName = String.format("CountMatchesForTable-%s", tableName);
    if(options.getEnableShardFiltering()) {
      List<String> shardsToInclude =
          Helpers.getShardListFromCommaSeparatedString(options.getShardsToInclude());
      countMatchesDoFn = new CountMatchesWithShardFilteringDoFn(shardsToInclude);
      stepName = String.format("CountMatchesWithShardFilteringForTable-%s", tableName);
    } else {
      countMatchesDoFn = new CountMatchesDoFn(tableSpec.getTimestampThresholdValue(),
          tableSpec.getTimestampThresholdDeltaInMins());
    }

    // Now tag the results by range
    PCollectionTuple countMatches = results.apply(
        stepName,
        ParDo.of(countMatchesDoFn)
            .withOutputTags(matchedRecordsTag,
                TupleTagList.of(unmatchedSpannerRecordsTag)
                    .and(unmatchedJDBCRecordsTag)
                    .and(sourceRecordsTag)
                    .and(targetRecordsTag)
                    .and(unmatchedSpannerRecordValuesTag)
                    .and(unmatchedJDBCRecordValuesTag)));

    // Count the tagged results by range
    PCollection<KV<String, Long>> matchedRecordCount =
        countMatches
            .get(matchedRecordsTag)
            .apply(String.format("MatchedCountForTable-%s", tableName), Count.perKey());

    PCollection<KV<String, Long>> unmatchedJDBCRecordCount =
        countMatches
            .get(unmatchedJDBCRecordsTag)
            .apply(String.format("UnmatchedCountForTable-%s", tableName), Count.perKey());

    PCollection<KV<String, Long>> unmatchedSpannerRecordCount =
        countMatches
            .get(unmatchedSpannerRecordsTag)
            .apply(String.format("UnmatchedSpannerCountForTable-%s", tableName), Count.perKey());

    PCollection<KV<String, Long>> sourceRecordCount =
        countMatches
            .get(sourceRecordsTag)
            .apply(String.format("UnmatchedJDBCCountForTable-%s", tableName), Count.perKey());

    PCollection<KV<String, Long>> targetRecordCount =
        countMatches
            .get(targetRecordsTag)
            .apply(String.format("TargetCountForTable-%s", tableName), Count.perKey());

    if(spannerConflictingRecordsWriter != null) {
      PCollection<HashResult> unmatchedSpannerValues =
          countMatches.get(unmatchedSpannerRecordValuesTag);

      unmatchedSpannerValues.apply(String.format("SpannerConflictingRecordsWriter-%s", tableName),
          spannerConflictingRecordsWriter);

      LOG.info("****** Writing spanner conflicting records");
    } else {
      LOG.info("****** Not writing spanner conflicting records");
    }

    if(jdbcConflictingRecordsWriter != null) {
      PCollection<HashResult> unmatchedJDBCValues =
          countMatches.get(unmatchedJDBCRecordValuesTag);

      unmatchedJDBCValues.apply(String.format("JDBCConflictingRecordsWriter-%s", tableName),
          jdbcConflictingRecordsWriter);

      LOG.info("****** Writing JDBC conflicting records");
    } else {
      LOG.info("****** Not writing JDBC conflicting records");
    }

    // group above counts by key
    PCollection<KV<String, CoGbkResult>> comparerResults =
        KeyedPCollectionTuple.of(matchedRecordCountTag, matchedRecordCount)
            .and(unmatchedSpannerRecordCountTag, unmatchedSpannerRecordCount)
            .and(unmatchedJDBCRecordCountTag, unmatchedJDBCRecordCount)
            .and(sourceRecordCountTag, sourceRecordCount)
            .and(targetRecordCountTag, targetRecordCount)
            .apply(String.format("GroupCountsByKeyForTable-%s", tableName), CoGroupByKey.create());

    String runName = options.getRunName();

    // assign grouped counts to object that can then be written to BQ
    PCollection<ComparerResult> reportOutput =
        comparerResults.apply(String.format("ReportOutputForTable-%s", tableName),
            ParDo.of(
            new DoFn<KV<String, CoGbkResult>, ComparerResult>() {
              String spannerDVTRunNamespace = String.format("SpannerDVT-%s", runName);
              Gauge matchedRecordsCounter = Metrics.gauge(spannerDVTRunNamespace, "matchedrecords");
              Gauge unmatchedJDBCRecordsCounter = Metrics.gauge(spannerDVTRunNamespace, "unmatchedjdbcrecords");
              Gauge unmatchedSpannerRecordsCounter = Metrics.gauge(spannerDVTRunNamespace, "unmatchedspannerrecords");
              Gauge sourceRecordCounter = Metrics.gauge(spannerDVTRunNamespace, "sourcerecords");
              Gauge targetRecordCounter = Metrics.gauge(spannerDVTRunNamespace, "targetrecords");
              @ProcessElement
              public void processElement(ProcessContext c) {
                ComparerResult comparerResult =
                    new ComparerResult(runName, c.element().getKey());

                comparerResult.matchCount =
                    getCountForTag(c.element().getValue(), matchedRecordCountTag);
                matchedRecordsCounter.set(comparerResult.matchCount);

                comparerResult.sourceConflictCount =
                    getCountForTag(c.element().getValue(), unmatchedJDBCRecordCountTag);
                unmatchedJDBCRecordsCounter.set(comparerResult.sourceConflictCount);

                comparerResult.targetConflictCount =
                    getCountForTag(c.element().getValue(), unmatchedSpannerRecordCountTag);
                unmatchedSpannerRecordsCounter.set(comparerResult.targetConflictCount);

                comparerResult.sourceCount =
                    getCountForTag(c.element().getValue(), sourceRecordCountTag);
                sourceRecordCounter.set(comparerResult.sourceCount);

                comparerResult.targetCount =
                    getCountForTag(c.element().getValue(), targetRecordCountTag);
                targetRecordCounter.set(comparerResult.targetCount);

                c.output(comparerResult);
              }
            }));

    reportOutput.apply(String.format("BQWriteForTable-%s", tableName),
        comparerResultWrite);
  }

  protected static Long getCountForTag(CoGbkResult result, TupleTag<Long> tag) {
    Iterable<Long> all = result.getAll(tag);

    if(all.iterator().hasNext()) return all.iterator().next();

    return 0L;
  }

  protected static PCollection<HashResult> getJDBCRecords(String tableName,
      PipelineTracker pipelineTracker,
      String query,
      Integer keyIndex,
      String rangeFieldType,
      DVTOptionsCore options,
      PCollection<PartitionRange> pRanges,
      Integer timestampThresholdKeyIndex,
      CustomTransformation customTransformation,
      Schema schema) {

    String driver = POSTGRES_JDBC_DRIVER;
    if(options.getProtocol().compareTo("mysql") == 0) {
      driver = MYSQL_JDBC_DRIVER;
    }

    // https://stackoverflow.com/questions/68353660/zero-date-value-prohibited-hibernate-sql-jpa
    String zeroDateTimeNullBehaviorStr = options.getZeroDateTimeBehavior() ? "?zeroDateTimeBehavior=CONVERT_TO_NULL" : "";

    // JDBC conn string
    String connString = String.format("jdbc:%s://%s:%d/%s%s", options.getProtocol(),
        options.getServer(),
        options.getPort(),
        options.getSourceDB(),
        zeroDateTimeNullBehaviorStr);

    LOG.info(String.format("++++++++++++++++++++++++++++++++JDBC conn string: %s", connString));

    String jdbcPass = Helpers.getJDBCPassword(options);

    //Return the ResultSet back for custom transformations instead of computing HashResult here.
    return getJDBCRecordsHelper(tableName,
        pipelineTracker,
        query,
        keyIndex,
        rangeFieldType,
        options,
        pRanges,
        timestampThresholdKeyIndex,
        customTransformation,
        schema,
        driver,
        connString,
        "0",
        options.getUsername(),
        jdbcPass);
  }

  @NotNull
  private static PCollection<HashResult> getJDBCRecordsHelper(String tableName,
      PipelineTracker pipelineTracker,
      String query,
      Integer keyIndex,
      String rangeFieldType,
      DVTOptionsCore options,
      PCollection<PartitionRange> pRanges,
      Integer timestampThresholdKeyIndex,
      CustomTransformation customTransformation,
      Schema schema,
      String driver,
      String connString,
      String shardId,
      String username,
      String jdbcPass) {

    Boolean outputParallelization = options.getEnableShuffle();

    if(options.getEnableShuffle()) {
      String reshuffleOpsStepName = String.format("ReshuffleJDBCForTable-%s",
          tableName);
      pRanges = pRanges.apply(reshuffleOpsStepName, Reshuffle.viaRandomKey());
    }

    pRanges = (PCollection<PartitionRange>) pipelineTracker.applyJDBCWait(pRanges);

    if(customTransformation != null) {
      PCollection<SourceRecord> jdbcRecordsSR =
          pRanges
              .apply(String.format("ReadInParallelForTable-%s", tableName),
                  JdbcIO.<PartitionRange, SourceRecord>readAll()
                      .withDataSourceProviderFn(
                          HikariPoolableDataSourceProvider.of(connString,
                              username,
                              jdbcPass,
                              driver,
                              options.getMaxJDBCConnectionsPerJVM()))
                      .withQuery(query)
                      .withDisableAutoCommit(false)
                      .withParameterSetter((input, preparedStatement) -> {
                        preparedStatement.setString(1, input.getStartRange());
                        preparedStatement.setString(2, input.getEndRange());
                      })
                      .withRowMapper(new SourceRecordMapper())
                      .withOutputParallelization(true)
              );

      CustomTransformationDoFn customTransformationDoFn = CustomTransformationDoFn.create(
          customTransformation,
          tableName,
          shardId,
          schema,
          keyIndex,
          rangeFieldType,
          options.getAdjustTimestampPrecision(),
          timestampThresholdKeyIndex);

      return jdbcRecordsSR.apply(String.format("CustomTransformationForTable-%s", tableName),
          ParDo.of(customTransformationDoFn));
    } else {
      PCollection<HashResult> jdbcRecords =
          pRanges
              .apply(String.format("ReadInParallelForTable-%s", tableName),
                  JdbcIO.<PartitionRange, HashResult>readAll()
                      .withDataSourceProviderFn(
                          HikariPoolableDataSourceProvider.of(connString,
                              username,
                              jdbcPass,
                              driver,
                              options.getMaxJDBCConnectionsPerJVM()))
                      .withQuery(query)
                      .withDisableAutoCommit(false)
                      .withParameterSetter((input, preparedStatement) -> {
                        preparedStatement.setString(1, input.getStartRange());
                        preparedStatement.setString(2, input.getEndRange());
                      })
                      .withRowMapper(new JDBCRowMapper(
                          keyIndex,
                          rangeFieldType,
                          options.getAdjustTimestampPrecision(),
                          timestampThresholdKeyIndex
                      ))
                      .withOutputParallelization(true)
              );

      pipelineTracker.addToJDBCReadList(jdbcRecords);

      return jdbcRecords;
    } // if/else
  }

  protected static PCollection<HashResult> getJDBCRecordsWithSharding(String tableName,
      PipelineTracker pipelineTracker,
      String query,
      Integer keyIndex,
      String rangeFieldType,
      DVTOptionsCore options,
      PCollection<PartitionRange> pRanges,
      Integer timestampThresholdIndex,
      CustomTransformation customTransformation,
      Schema schema) {

    String driver = POSTGRES_JDBC_DRIVER;
    if(options.getProtocol().compareTo("mysql") == 0) {
      driver = MYSQL_JDBC_DRIVER;
    }

    String shardConfigurationFileUrl = options.getSourceConfigURL();

    List<Shard> shards = new ShardFileReader(new SecretManagerAccessorImpl()).readShardingConfig(shardConfigurationFileUrl);
    LOG.info("Total shards read: {}", shards.size());
    ArrayList<PCollection<HashResult>> pCollections = new ArrayList<>();

    for(Shard shard: shards) {

      // https://stackoverflow.com/questions/68353660/zero-date-value-prohibited-hibernate-sql-jpa
      String zeroDateTimeNullBehaviorStr = options.getZeroDateTimeBehavior() ? "?zeroDateTimeBehavior=CONVERT_TO_NULL" : "";

      // JDBC conn string
      String connString = String.format("jdbc:%s://%s:%d/%s%s", options.getProtocol(),
          shard.getHost(),
          Integer.parseInt(shard.getPort()),
          shard.getDbName(),
          zeroDateTimeNullBehaviorStr);

      PCollection<HashResult> hashedJDBCRecordsPerShard = getJDBCRecordsHelper(
          tableName,
          pipelineTracker,
          query,
          keyIndex,
          rangeFieldType,
          options,
          pRanges,
          timestampThresholdIndex,
          customTransformation,
          schema,
          driver,
          connString,
          shard.getLogicalShardId(),
          shard.getUserName(),
          shard.getPassword());

      pCollections.add(hashedJDBCRecordsPerShard);
    } // for

    String flattenStepName = String.format("FlattenJDBCRecordsForTable-%s", tableName);
    PCollection<HashResult> mergedJdbcRecords =
        PCollectionList.of(pCollections).apply(flattenStepName, Flatten.pCollections());

    return mergedJdbcRecords;
  }

  protected static PCollection<HashResult> getSpannerRecords(String tableName,
      PipelineTracker pipelineTracker,
      String query,
      Integer keyIndex,
      String rangeFieldType,
      DVTOptionsCore options,
      PCollection<PartitionRange> pRanges,
      Integer timestampThresholdIndex) {

    Boolean adjustTimestampPrecision = options.getAdjustTimestampPrecision();

    String readOpsStepName = String.format("ConvertToSpannerIOReadOperationsForTable-%s",
        tableName);

    pRanges = (PCollection<PartitionRange>) pipelineTracker.applySpannerWait(pRanges);

    if(options.getEnableShuffle()) {
      String reshuffleOpsStepName = String.format("ReshuffleSpannerForTable-%s",
          tableName);
      pRanges = pRanges.apply(reshuffleOpsStepName, Reshuffle.viaRandomKey());
    }

    // https://cloud.google.com/spanner/docs/samples/spanner-dataflow-readall
    PCollection<ReadOperation> readOps = pRanges
        .apply(readOpsStepName,
        MapElements.into(TypeDescriptor.of(ReadOperation.class))
        .via(
            (SerializableFunction<PartitionRange, ReadOperation>)
                input -> {
                  Statement statement;
                  switch(rangeFieldType) {
                    case TableSpec.UUID_FIELD_TYPE:
                    case TableSpec.TIMESTAMP_FIELD_TYPE:
                    case TableSpec.STRING_FIELD_TYPE:
                      statement =
                          Statement.newBuilder(query)
                              .bind("p1")
                              .to(input.getStartRange())
                              .bind("p2")
                              .to(input.getEndRange())
                              .build();
                      break;
                    case TableSpec.INT_FIELD_TYPE:
                      statement =
                          Statement.newBuilder(query)
                              .bind("p1")
                              .to(Integer.parseInt(input.getStartRange()))
                              .bind("p2")
                              .to(Integer.parseInt(input.getEndRange()))
                              .build();
                      break;
                    case TableSpec.LONG_FIELD_TYPE:
                      statement =
                          Statement.newBuilder(query)
                              .bind("p1")
                              .to(Long.parseLong(input.getStartRange()))
                              .bind("p2")
                              .to(Long.parseLong(input.getEndRange()))
                              .build();
                      break;
                    default:
                      throw new RuntimeException(String.format("Unexpected range field type: %s",
                          rangeFieldType));
                  }
                  ReadOperation readOperation =
                      ReadOperation.create()
                          .withQuery(statement);

                  return readOperation;
                }));

    String spannerProjectId = options.getSpannerProjectId().isEmpty() ?
        options.getProjectId() : options.getSpannerProjectId();

    String spannerReadStepName = String.format("SpannerReadAllForTable-%s", tableName);

    ReadAll spannerRead = SpannerIO.readAll();
    if(options.getReadFromSpannerWithHighPriorityCPU()) {
      spannerRead = spannerRead.withHighPriority();
    }

    if(options.getPerformStrongReadAtSpanner()) {
      spannerRead = spannerRead.withTimestampBound(TimestampBound.strong());
    } else {
      spannerRead = spannerRead.withTimestampBound(TimestampBound.ofExactStaleness(
          options.getMaxStalenessInSeconds(),
          TimeUnit.SECONDS));
    }

    PCollection<Struct> spannerRecords =
        readOps.apply(spannerReadStepName, spannerRead
        .withProjectId(spannerProjectId)
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getSpannerDatabaseId()));

    Long ddrCount = options.getDdrCount();
    String serviceNameForShardCalc = options.getServiceNameForShardCalc();
    String colNameForShardCalc = options.getColNameForShardCalc();
    Boolean enableShardFiltering = options.getEnableShardFiltering();

    String convertToHashResultStepName =
        String.format("ConvertToHashResultForTable-%s", tableName);
    PCollection<HashResult> spannerHashes = spannerRecords.apply(convertToHashResultStepName,
        MapElements.into(TypeDescriptor.of(HashResult.class))
            .via(
                (SerializableFunction<? super Struct, HashResult>)
                    input -> HashResult.fromSpannerStruct(input,
                        keyIndex,
                        rangeFieldType,
                        adjustTimestampPrecision,
                        timestampThresholdIndex,
                        ddrCount,
                        serviceNameForShardCalc,
                        tableName,
                        colNameForShardCalc,
                        enableShardFiltering)
            ));

    return spannerHashes;
  }

  private static List<PartitionRange> getPartitionRanges(TableSpec tableSpec,
      Integer partitionCount,
      Integer partitionFilterRatio) {
    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(tableSpec.getRangeFieldType());
    List<PartitionRange> bRanges;

    LOG.info(String.format("Partition count is %d for Table %s",
        partitionCount,
        tableSpec.getTableName()));

    if(partitionFilterRatio > 0) {
      LOG.info("Getting partition ranges w/ filtering");
      bRanges = fetcher.getPartitionRangesWithPartitionFilter(tableSpec.getRangeStart(),
          tableSpec.getRangeEnd(),
          partitionCount,
          partitionFilterRatio);
    } else {
      LOG.info("Getting partition ranges w/ coverage");

      bRanges = fetcher.getPartitionRangesWithCoverage(tableSpec.getRangeStart(),
          tableSpec.getRangeEnd(),
          partitionCount,
          tableSpec.getRangeCoverage());
    }

    return bRanges;
  }

  private static List<TableSpec> generateTableSpec(DVTOptionsCore options) {
    String tableSpecJson = options.getTableSpecJson();
    String sessionFileJson = options.getSessionFileJson();
    Boolean isGenerateTableSpecFlagEnabled = options.getGenerateTableSpec();
    List<TableSpec> tableSpecs = new ArrayList<>();
    //if generateSpec is enabled, create the tableSpec from the one that is specified,
    //or merge them if both are specified.
    if (isGenerateTableSpecFlagEnabled) {
      List<TableSpec> tableSpecListFromTableSpecJson = null;
      List<TableSpec> tableSpecListFromSessionFileJson = null;
      if (!Helpers.isNullOrEmpty(sessionFileJson)) {
        tableSpecListFromSessionFileJson = TableSpecList.getFromSessionFile(options);
      }
      if (!Helpers.isNullOrEmpty(tableSpecJson)) {
        tableSpecListFromTableSpecJson = TableSpecList.getFromJsonFile(options.getProjectId(),
            tableSpecJson);
      }

      if (tableSpecListFromSessionFileJson != null && tableSpecListFromTableSpecJson != null) {
        LOG.warn(
            "GenerateTableSpec mode: Session file and tableSpec have both been specified! TableSpec will take "
                + "precedence over session file for the tables for which it is defined!!");
        List<String> tableNamesFromTableSpecJson = tableSpecListFromTableSpecJson.stream()
            .map(TableSpec::getTableName)
            .collect(Collectors.toList());

        List<TableSpec> filteredTableSpecs = tableSpecListFromSessionFileJson.stream()
            .filter(tableSpec -> !tableNamesFromTableSpecJson.contains(tableSpec.getTableName()))
            .collect(Collectors.toList());

        tableSpecs.addAll(filteredTableSpecs);
        tableSpecs.addAll(tableSpecListFromTableSpecJson);
      } else if (!Helpers.isNullOrEmpty(tableSpecJson)) {
        tableSpecs = tableSpecListFromTableSpecJson;
      } else if (!Helpers.isNullOrEmpty(sessionFileJson)) {
        tableSpecs = tableSpecListFromSessionFileJson;
      } else {
        tableSpecs = getTableSpecs();
      }
    } else {
      //if generateSpec is disabled, return the tableSpec if it is specified,
      //otherwise, use the session file one.
      if (!Helpers.isNullOrEmpty(tableSpecJson)) {
        tableSpecs = TableSpecList.getFromJsonFile(options.getProjectId(), tableSpecJson);
      } else if (!Helpers.isNullOrEmpty(sessionFileJson)) {
        tableSpecs = TableSpecList.getFromJsonFile(options.getProjectId(), tableSpecJson);
      } else {
        tableSpecs = getTableSpecs();
      }
    }
    return tableSpecs;
  }

  public static void runDVT(DVTOptionsCore options) throws IOException {
    Pipeline p = Pipeline.create(options);
    if (options.getGenerateTableSpec()) {
      String sessionFileJson = options.getSessionFileJson();
      if (Helpers.isNullOrEmpty(sessionFileJson)) {
        throw new RuntimeException("Session file needs to be provided to generate the tableSpec from it!");
      }
      List<TableSpec> tableSpecs = generateTableSpec(options);
      String jsonFileName = String.format("%s-tableSpec-%s.json", options.getSpannerDatabaseId(), System.currentTimeMillis());
      TableSpecList.toJsonFile(tableSpecs, jsonFileName);
      LOG.info("TableSpec has been written to {} file", jsonFileName);
      return;
    }

    p.getCoderRegistry()
        .registerCoderForClass(HashResult.class, AvroCoder.of(HashResult.class));
    p.getCoderRegistry()
        .registerCoderForClass(FilteryByShard.class, AvroCoder.of(FilteryByShard.class));

    if(Helpers.isNullOrEmpty(options.getRunName())) {
      DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss");
      String timestampStr = DateTime.now().toString(formatter);
      options.setRunName(String.format("Run-%s", timestampStr));
    }

    CustomTransformation customTransformation = null;

    if(!Helpers.isNullOrEmpty(options.getTransformationJarPath())) {
      if (!Helpers.isNullOrEmpty(options.getTransformationJarPath()) && !Helpers.isNullOrEmpty(options.getTransformationClassName()) && Helpers.isNullOrEmpty(options.getSessionFileJson())) {
        throw new RuntimeException("Custom transformations is only supported with session file integration. Please specify the session file and re-run the pipeline");
      }

      customTransformation = CustomTransformation
          .builder(options.getTransformationJarPath(), options.getTransformationClassName())
          .setCustomParameters(options.getTransformationCustomParameters())
          .build();
    }

    Schema schema = null;
    if (!Helpers.isNullOrEmpty(options.getSessionFileJson())) {
      schema = SessionFileReader.read(options.getSessionFileJson());
    }

    List<TableSpec> tableSpecs = generateTableSpec(options);

    PipelineTracker pipelineTracker = new PipelineTracker();
    pipelineTracker.setMaxTablesInEffectAtOneTime(options.getMaxTablesInEffectAtOneTime());

    for(TableSpec tableSpec: tableSpecs) {
      BigQueryIO.Write<ComparerResult> comparerResultWrite =
          getComparisonResultsBQWriter(options, tableSpec.getTableName());

      BigQueryIO.Write<HashResult> jdbcConflictingRecordsWriter = null;
      BigQueryIO.Write<HashResult> spannerConflictingRecordsWriter = null;

      String conflictingRecordsBQTableName = options.getConflictingRecordsBQTableName();
      if(!Helpers.isNullOrEmpty(conflictingRecordsBQTableName)) {
        LOG.info(String.format("*******Enabling writing of conflicting records to table %s",
            conflictingRecordsBQTableName));
        spannerConflictingRecordsWriter = getConflictingRecordsBQWriter(options,
            options.getRunName(),
            tableSpec.getTableName(),
            "spanner");

        jdbcConflictingRecordsWriter = getConflictingRecordsBQWriter(options,
            options.getRunName(),
            tableSpec.getTableName(),
            "jdbc");
      } else {
        LOG.info(String.format("*******NOT enabling writing of conflicting records! %s",
            conflictingRecordsBQTableName));
      }

      configureComparisonPipeline(p,
          pipelineTracker,
          options,
          tableSpec,
          comparerResultWrite,
          jdbcConflictingRecordsWriter,
          spannerConflictingRecordsWriter,
          customTransformation,
          schema);
    }

    p.run();
  }

  public static void main(String[] args) throws IOException {
    JDBCToSpannerDVTWithHashOptions options =
        PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(JDBCToSpannerDVTWithHashOptions.class);

    JDBCToSpannerDVTWithHash dvtApp = new JDBCToSpannerDVTWithHash();
    dvtApp.runDVT(options);
  }
} // class JDBCToSpannerDVTWithHash
