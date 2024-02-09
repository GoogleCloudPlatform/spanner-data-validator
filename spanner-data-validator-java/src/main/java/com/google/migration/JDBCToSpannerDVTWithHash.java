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
import com.google.migration.common.DVTOptionsCore;
import com.google.migration.common.JDBCRowMapper;
import com.google.migration.dofns.CountMatchesDoFn;
import com.google.migration.dofns.MapWithRangeFn;
import com.google.migration.dofns.MapWithRangeFn.MapWithRangeType;
import com.google.migration.dto.ComparerResult;
import com.google.migration.dto.HashResult;
import com.google.migration.dto.PartitionRange;
import com.google.migration.dto.ShardSpec;
import com.google.migration.dto.TableSpec;
import com.google.migration.partitioning.PartitionRangeListFetcher;
import com.google.migration.partitioning.PartitionRangeListFetcherFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
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
            .setMode("REQUIRED"));
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
                .set("hash", x.sha256))
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
        .withSchema(bqSchema)
        .withWriteDisposition(WriteDisposition.WRITE_APPEND)
        .withMethod(Write.Method.STORAGE_WRITE_API);

    return bqWrite;
  }

  protected static void configureComparisonPipeline(Pipeline p,
      DVTOptionsCore options,
      TableSpec tableSpec,
      BigQueryIO.Write<ComparerResult> comparerResultWrite,
      BigQueryIO.Write<HashResult> jdbcConflictingRecordsWriter,
      BigQueryIO.Write<HashResult> spannerConflictingRecordsWriter) {

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
    String shardSpecJsonFile = options.getShardSpecJson();

    String createRangesForTableStep = String.format("CreateRangesForTable-%s", tableName);
    PCollection<PartitionRange> pRanges = p.apply(createRangesForTableStep, Create.of(bRanges));

    // get ranges of keys
    String partitionRangesViewStep = String.format("PartitionRangesForTable-%s", tableName);
    final PCollectionView<List<PartitionRange>> partitionRangesView =
        pRanges.apply(partitionRangesViewStep, View.asList());

    PCollection<HashResult> spannerRecords =
        getSpannerRecords(tableName,
            tableSpec.getDestQuery(),
            tableSpec.getRangeFieldIndex(),
            tableSpec.getRangeFieldType(),
            options,
            pRanges);

    // Map Range [start, end) + hash => HashResult (spanner)
    String mapWithRangesForSpannerStep =
        String.format("MapWithRangesSpannerRecordsForTable-%s", tableName);
    PCollection<KV<String, HashResult>> mappedWithHashSpannerRecords =
        spannerRecords.apply(mapWithRangesForSpannerStep, ParDo.of(new MapWithRangeFn(partitionRangesView,
                MapWithRangeType.RANGE_PLUS_HASH,
                tableSpec.getRangeFieldType()))
            .withSideInputs(partitionRangesView));

    PCollection<HashResult> jdbcRecords;

    if(Helpers.isNullOrEmpty(shardSpecJsonFile)) {
      jdbcRecords =
          getJDBCRecords(tableName,
              tableSpec.getSourceQuery(),
              tableSpec.getRangeFieldIndex(),
              tableSpec.getRangeFieldType(),
              options,
              pRanges);
    } else {
      jdbcRecords =
          getJDBCRecordsWithSharding(tableName,
              tableSpec.getSourceQuery(),
              tableSpec.getRangeFieldIndex(),
              tableSpec.getRangeFieldType(),
              options,
              pRanges);
    }

    // Map Range [start, end) + hash => HashResult (JDBC)
    String mapWithRangesForJDBCStep =
        String.format("MapWithRangesJDBCRecordsForTable-%s", tableName);
    PCollection<KV<String, HashResult>> mappedWithHashJdbcRecords =
        jdbcRecords.apply(mapWithRangesForJDBCStep, ParDo.of(new MapWithRangeFn(partitionRangesView,
                MapWithRangeType.RANGE_PLUS_HASH,
                tableSpec.getRangeFieldType()))
            .withSideInputs(partitionRangesView));

    // Group by range [start, end) + hash => {JDBC HashResult if it exists, Spanner HashResult if it exists}
    String groupByKeyStep = String.format("GroupByKeyForTable-%s", tableName);
    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(jdbcTag, mappedWithHashJdbcRecords)
            .and(spannerTag, mappedWithHashSpannerRecords)
            .apply(groupByKeyStep, CoGroupByKey.create());

    // Now tag the results by range
    PCollectionTuple countMatches = results.apply(String.format("CountMatchesForTable-%s", tableName),
        ParDo.of(new CountMatchesDoFn()).withOutputTags(matchedRecordsTag,
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
    }

    if(jdbcConflictingRecordsWriter != null) {
      PCollection<HashResult> unmatchedJDBCValues =
          countMatches.get(unmatchedJDBCRecordValuesTag);

      unmatchedJDBCValues.apply(String.format("JDBCConflictingRecordsWriter-%s", tableName),
          jdbcConflictingRecordsWriter);
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
              @ProcessElement
              public void processElement(ProcessContext c) {
                ComparerResult comparerResult =
                    new ComparerResult(runName, c.element().getKey());

                comparerResult.matchCount =
                    getCountForTag(c.element().getValue(), matchedRecordCountTag);

                comparerResult.sourceConflictCount =
                    getCountForTag(c.element().getValue(), unmatchedJDBCRecordCountTag);

                comparerResult.targetConflictCount =
                    getCountForTag(c.element().getValue(), unmatchedSpannerRecordCountTag);

                comparerResult.sourceCount =
                    getCountForTag(c.element().getValue(), sourceRecordCountTag);

                comparerResult.targetCount =
                    getCountForTag(c.element().getValue(), targetRecordCountTag);

                c.output(comparerResult);
              }
            }));

    reportOutput.apply(String.format("BQWriteForTable-%s", tableName), comparerResultWrite);
  }

  protected static Long getCountForTag(CoGbkResult result, TupleTag<Long> tag) {
    Iterable<Long> all = result.getAll(tag);

    if(all.iterator().hasNext()) return all.iterator().next();

    return 0L;
  }

  protected static PCollection<HashResult> getJDBCRecords(String tableName,
      String query,
      Integer keyIndex,
      String rangeFieldType,
      DVTOptionsCore options,
      PCollection<PartitionRange> pRanges) {

    Boolean adjustTimestampPrecision = options.getAdjustTimestampPrecision();

    String driver = POSTGRES_JDBC_DRIVER;
    if(options.getProtocol().compareTo("mysql") == 0) {
      driver = MYSQL_JDBC_DRIVER;
    }

    // JDBC conn string
    String connString = String.format("jdbc:%s://%s:%d/%s", options.getProtocol(),
        options.getServer(),
        options.getPort(),
        options.getSourceDB());

    String jdbcPass = Helpers.getJDBCPassword(options);

    PCollection<HashResult> jdbcRecords =
        pRanges.apply(String.format("ReadInParallelForTable-%s", tableName),
            JdbcIO.<PartitionRange, HashResult>readAll()
                .withDataSourceConfiguration(DataSourceConfiguration.create(
                        driver, connString)
                    .withUsername(options.getUsername())
                    .withPassword(jdbcPass))
                .withQuery(query)
                .withParameterSetter((input, preparedStatement) -> {
                  preparedStatement.setString(1, input.getStartRange());
                  preparedStatement.setString(2, input.getEndRange());
                })
                .withRowMapper(new JDBCRowMapper(keyIndex,
                    rangeFieldType,
                    adjustTimestampPrecision))
                .withOutputParallelization(false)
        );

    return  jdbcRecords;
  }

  protected static PCollection<HashResult> getJDBCRecordsWithSharding(String tableName,
      String query,
      Integer keyIndex,
      String rangeFieldType,
      DVTOptionsCore options,
      PCollection<PartitionRange> pRanges) {

    Boolean adjustTimestampPrecision = options.getAdjustTimestampPrecision();

    String driver = POSTGRES_JDBC_DRIVER;
    if(options.getProtocol().compareTo("mysql") == 0) {
      driver = MYSQL_JDBC_DRIVER;
    }

    String shardSpecJsonFile = options.getShardSpecJson();

    List<ShardSpec> shardSpecs = ShardSpecList.getShardSpecsFromJsonFile(shardSpecJsonFile);
    ArrayList<PCollection<HashResult>> pCollections = new ArrayList<>();

    String jdbcPass = Helpers.getJDBCPassword(options);

    for(ShardSpec shardSpec: shardSpecs) {

      // JDBC conn string
      String connString = String.format("jdbc:%s://%s:%d/%s", options.getProtocol(),
          shardSpec.getHost(),
          options.getPort(),
          shardSpec.getDb());

      PCollection<HashResult> jdbcRecords =
          pRanges.apply(String.format("ReadInParallelWithShardsForTable-%s", tableName),
              JdbcIO.<PartitionRange, HashResult>readAll()
                  .withDataSourceConfiguration(DataSourceConfiguration.create(
                          driver, connString)
                      .withUsername(shardSpec.getUser())
                      .withPassword(jdbcPass))
                  .withQuery(query)
                  .withParameterSetter((input, preparedStatement) -> {
                    preparedStatement.setString(1, input.getStartRange());
                    preparedStatement.setString(2, input.getEndRange());
                  })
                  .withRowMapper(
                      new JDBCRowMapper(keyIndex, rangeFieldType, adjustTimestampPrecision))
                  .withOutputParallelization(false)
          );

      pCollections.add(jdbcRecords);
    } // for

    String flattenStepName = String.format("FlattenJDBCRecordsForTable-%s", tableName);
    PCollection<HashResult> mergedJdbcRecords =
        PCollectionList.of(pCollections).apply(flattenStepName, Flatten.pCollections());

    return mergedJdbcRecords;
  }

  protected static PCollection<HashResult> getSpannerRecords(String tableName,
      String query,
      Integer keyIndex,
      String rangeFieldType,
      DVTOptionsCore options,
      PCollection<PartitionRange> pRanges) {

    Boolean adjustTimestampPrecision = options.getAdjustTimestampPrecision();

    String readOpsStepName = String.format("ConvertToSpannerIOReadOperationsForTable-%s",
        tableName);
    // https://cloud.google.com/spanner/docs/samples/spanner-dataflow-readall
    PCollection<ReadOperation> readOps = pRanges.apply(readOpsStepName,
        MapElements.into(TypeDescriptor.of(ReadOperation.class))
        .via(
            (SerializableFunction<PartitionRange, ReadOperation>)
                input -> {
                  Statement statement;
                  switch(rangeFieldType) {
                    case TableSpec.UUID_FIELD_TYPE:
                    case TableSpec.TIMESTAMP_FIELD_TYPE:
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
                      ReadOperation.create().withQuery(statement);

                  return readOperation;
                }));

    String spannerReadStepName = String.format("SpannerReadAllForTable-%s", tableName);
    PCollection<Struct> spannerRecords =
        readOps.apply(spannerReadStepName, SpannerIO.readAll()
        .withProjectId(options.getProjectId())
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getSpannerDatabaseId()));

    String convertToHashResultStepName =
        String.format("ConvertToHashResultForTable-%s", tableName);
    PCollection<HashResult> spannerHashes = spannerRecords.apply(convertToHashResultStepName,
        MapElements.into(TypeDescriptor.of(HashResult.class))
            .via(
                (SerializableFunction<? super Struct, HashResult>)
                    input -> HashResult.fromSpannerStruct(input,
                        keyIndex,
                        rangeFieldType,
                        adjustTimestampPrecision)
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

  public static void runDVT(DVTOptionsCore options) throws IOException {
    Pipeline p = Pipeline.create(options);

    p.getCoderRegistry().registerCoderForClass(HashResult.class, AvroCoder.of(HashResult.class));

    if(Helpers.isNullOrEmpty(options.getRunName())) {
      DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss");
      String timestampStr = DateTime.now().toString(formatter);
      options.setRunName(String.format("Run-%s", timestampStr));
    }

    List<TableSpec> tableSpecs = getTableSpecs();
    String tableSpecJson = options.getTableSpecJson();
    if(!Helpers.isNullOrEmpty(tableSpecJson)) {
      tableSpecs = TableSpecList.getFromJsonFile(tableSpecJson);
    }

    for(TableSpec tableSpec: tableSpecs) {
      BigQueryIO.Write<ComparerResult> comparerResultWrite =
          getComparisonResultsBQWriter(options, tableSpec.getTableName());

      BigQueryIO.Write<HashResult> jdbcConflictingRecordsWriter = null;
      BigQueryIO.Write<HashResult> spannerConflictingRecordsWriter = null;

      String conflictingRecordsBQTableName = options.getConflictingRecordsBQTableName();
      if(!Helpers.isNullOrEmpty(conflictingRecordsBQTableName)) {
        LOG.info(String.format("Enabling writing of conflicting records to table %s",
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
        LOG.info(String.format("NOT enabling writing of conflicting records! %s",
            conflictingRecordsBQTableName));
      }

      configureComparisonPipeline(p,
          options,
          tableSpec,
          comparerResultWrite,
          jdbcConflictingRecordsWriter,
          spannerConflictingRecordsWriter);
    }

    p.run().waitUntilFinish();
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