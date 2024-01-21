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
import static com.google.migration.SharedTags.unmatchedJDBCRecordsTag;
import static com.google.migration.SharedTags.unmatchedSpannerRecordCountTag;
import static com.google.migration.SharedTags.unmatchedSpannerRecordsTag;
import static com.google.migration.TableSpecList.getTableSpecs;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.migration.TableSpecList.ShardSpecList;
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
import java.sql.ResultSet;
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
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
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
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToSpannerDVTWithHash {
  protected static final String POSTGRES_JDBC_DRIVER = "org.postgresql.Driver";
  protected static final String MYSQL_JDBC_DRIVER = "com.mysql.jdbc.Driver";
  private static final Logger LOG = LoggerFactory.getLogger(JDBCToSpannerDVTWithHash.class);

  // [START JDBCToSpannerDVTWithHash_options]
  public interface JDBCToSpannerDVTWithHashOptions extends DVTOptionsCore {
  }
  // [END JDBCToSpannerDVTWithHash_options]

  protected static List<TableFieldSchema> getBQWriteCols() {
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

  protected static BigQueryIO.Write<ComparerResult> getBQWrite(DVTOptionsCore options,
      String tableName) {

    TableSchema bqSchema = new TableSchema().setFields(getBQWriteCols());

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

  protected static void configureComparisonPipeline(Pipeline p,
      DVTOptionsCore options,
      TableSpec tableSpec,
      BigQueryIO.Write<ComparerResult> bqWrite) {

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(tableSpec.getRangeFieldType());
    List<PartitionRange> bRanges = fetcher.getPartitionRangesWithCoverage(tableSpec.getRangeStart(),
        tableSpec.getRangeEnd(),
        options.getPartitionCount(),
        tableSpec.getRangeCoverage());

    String tableName = tableSpec.getTableName();
    Boolean supportShardedSource = options.getSupportShardedSource();

    PCollection<PartitionRange> pRanges = p.apply(Create.of(bRanges));

    // get ranges of keys
    final PCollectionView<List<PartitionRange>> uuidRangesView = pRanges.apply(View.asList());

    PCollection<HashResult> spannerRecords =
        getSpannerRecords(tableSpec.getDestQuery(),
            tableSpec.getRangeFieldIndex(),
            tableSpec.getRangeFieldType(),
            options,
            pRanges);
    PCollection<KV<String, HashResult>> mappedWithHashSpannerRecords =
        spannerRecords.apply(ParDo.of(new MapWithRangeFn(uuidRangesView,
                MapWithRangeType.RANGE_PLUS_HASH,
                tableSpec.getRangeFieldType()))
            .withSideInputs(uuidRangesView));

    PCollection<HashResult> jdbcRecords;

    if(!supportShardedSource) {
      jdbcRecords =
          getJDBCRecords(tableSpec.getSourceQuery(),
              tableSpec.getRangeFieldIndex(),
              tableSpec.getRangeFieldType(),
              options,
              pRanges);
    } else {
      jdbcRecords =
          getJDBCRecordsWithSharding(tableSpec.getSourceQuery(),
              tableSpec.getRangeFieldIndex(),
              tableSpec.getRangeFieldType(),
              options,
              pRanges);
    }

    PCollection<KV<String, HashResult>> mappedWithHashJdbcRecords =
        jdbcRecords.apply(ParDo.of(new MapWithRangeFn(uuidRangesView,
                MapWithRangeType.RANGE_PLUS_HASH,
                tableSpec.getRangeFieldType()))
            .withSideInputs(uuidRangesView));

    PCollection<KV<String, CoGbkResult>> results =
        KeyedPCollectionTuple.of(jdbcTag, mappedWithHashJdbcRecords)
            .and(spannerTag, mappedWithHashSpannerRecords)
            .apply(CoGroupByKey.create());

    PCollectionTuple countMatches = results.apply(String.format("Countmatches-%s", tableName),
        ParDo.of(new CountMatchesDoFn()).withOutputTags(matchedRecordsTag,
            TupleTagList.of(unmatchedSpannerRecordsTag)
                .and(unmatchedJDBCRecordsTag)
                .and(sourceRecordsTag)
                .and(targetRecordsTag)));

    PCollection<KV<String, Long>> matchedRecordCount =
        countMatches.get(matchedRecordsTag).apply(Count.perKey());

    PCollection<KV<String, Long>> unmatchedJDBCRecordCount =
        countMatches.get(unmatchedJDBCRecordsTag).apply(Count.perKey());

    PCollection<KV<String, Long>> unmatchedSpannerRecordCount =
        countMatches.get(unmatchedSpannerRecordsTag).apply(Count.perKey());

    PCollection<KV<String, Long>> sourceRecordCount =
        countMatches.get(sourceRecordsTag).apply(Count.perKey());

    PCollection<KV<String, Long>> targetRecordCount =
        countMatches.get(targetRecordsTag).apply(Count.perKey());

    PCollection<KV<String, CoGbkResult>> comparerResults =
        KeyedPCollectionTuple.of(matchedRecordCountTag, matchedRecordCount)
            .and(unmatchedSpannerRecordCountTag, unmatchedSpannerRecordCount)
            .and(unmatchedJDBCRecordCountTag, unmatchedJDBCRecordCount)
            .and(sourceRecordCountTag, sourceRecordCount)
            .and(targetRecordCountTag, targetRecordCount)
            .apply(CoGroupByKey.create());

    String runName = options.getRunName();

    PCollection<ComparerResult> reportOutput =
        comparerResults.apply(String.format("reportOutput-%s", tableName),
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

    reportOutput.apply(bqWrite);
  }

  protected static Long getCountForTag(CoGbkResult result, TupleTag<Long> tag) {
    Iterable<Long> all = result.getAll(tag);

    if(all.iterator().hasNext()) return all.iterator().next();

    return 0L;
  }

  protected static PCollection<HashResult> getJDBCRecords(String query,
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

    PCollection<HashResult> jdbcRecords =
        pRanges.apply(String.format("Read%sInParallel", "MyTable"),
            JdbcIO.<PartitionRange, HashResult>readAll()
                .withDataSourceConfiguration(DataSourceConfiguration.create(
                        driver, connString)
                    .withUsername(options.getUsername())
                    .withPassword(options.getPassword()))
                .withQuery(query)
                .withParameterSetter((input, preparedStatement) -> {
                  preparedStatement.setString(1, input.getStartRange());
                  preparedStatement.setString(2, input.getEndRange());
                })
                .withRowMapper(new JDBCRowMapper(keyIndex, rangeFieldType, adjustTimestampPrecision))
                .withOutputParallelization(false)
        );

    return  jdbcRecords;
  }

  protected static PCollection<HashResult> getJDBCRecordsWithSharding(String query,
      Integer keyIndex,
      String rangeFieldType,
      DVTOptionsCore options,
      PCollection<PartitionRange> pRanges) {

    Boolean adjustTimestampPrecision = options.getAdjustTimestampPrecision();

    String driver = POSTGRES_JDBC_DRIVER;
    if(options.getProtocol().compareTo("mysql") == 0) {
      driver = MYSQL_JDBC_DRIVER;
    }

    List<ShardSpec> shardSpecs = ShardSpecList.getShardSpecs(options);
    ArrayList<PCollection<HashResult>> pCollections = new ArrayList<>();

    for(ShardSpec shardSpec: shardSpecs) {

      // JDBC conn string
      String connString = String.format("jdbc:%s://%s:%d/%s", options.getProtocol(),
          shardSpec.getHost(),
          options.getPort(),
          shardSpec.getDb());

      PCollection<HashResult> jdbcRecords =
          pRanges.apply(String.format("Read%sInParallel", "MyTable"),
              JdbcIO.<PartitionRange, HashResult>readAll()
                  .withDataSourceConfiguration(DataSourceConfiguration.create(
                          driver, connString)
                      .withUsername(shardSpec.getUser())
                      .withPassword(shardSpec.getPassword()))
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

    PCollection<HashResult> mergedJdbcRecords =
        PCollectionList.of(pCollections).apply(Flatten.pCollections());

    return mergedJdbcRecords;
  }

  protected static PCollection<HashResult> getSpannerRecords(String query,
      Integer keyIndex,
      String rangeFieldType,
      DVTOptionsCore options,
      PCollection<PartitionRange> pRanges) {

    Boolean adjustTimestampPrecision = options.getAdjustTimestampPrecision();

    // https://cloud.google.com/spanner/docs/samples/spanner-dataflow-readall
    PCollection<ReadOperation> readOps = pRanges.apply("ConvertToSpannerIOReadOperations",
        MapElements.into(TypeDescriptor.of(ReadOperation.class))
        .via(
            (SerializableFunction<PartitionRange, ReadOperation>)
                input -> {
                  Statement statement;
                  switch(rangeFieldType) {
                    case TableSpec.UUID_FIELD_TYPE:
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
                      throw new RuntimeException(String.format("Unexpected range field type: %s", rangeFieldType));
                  }
                  ReadOperation readOperation =
                      ReadOperation.create().withQuery(statement);

                  return readOperation;
                }));

    PCollection<Struct> spannerRecords =
        readOps.apply("SpannerReadAll", SpannerIO.readAll()
        .withProjectId(options.getProjectId())
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getSpannerDatabaseId()));

    PCollection<HashResult> spannerHashes = spannerRecords.apply("ConvertToHashResult",
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

  public static void runDVT(DVTOptionsCore options) {
    Pipeline p = Pipeline.create(options);

    p.getCoderRegistry().registerCoderForClass(HashResult.class, AvroCoder.of(HashResult.class));

    if(Helpers.isNullOrEmpty(options.getRunName())) {
      DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss");
      String timestampStr = DateTime.now().toString(formatter);
      options.setRunName(String.format("Run-%s", timestampStr));
    }

    List<TableSpec> tableSpecs = getTableSpecs();

    for(TableSpec tableSpec: tableSpecs) {
      BigQueryIO.Write<ComparerResult> bqWrite = getBQWrite(options, tableSpec.getTableName());

      configureComparisonPipeline(p, options, tableSpec, bqWrite);
    }

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    JDBCToSpannerDVTWithHashOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JDBCToSpannerDVTWithHashOptions.class);

    JDBCToSpannerDVTWithHash dvtApp = new JDBCToSpannerDVTWithHash();
    dvtApp.runDVT(options);
  }
} // class JDBCToSpannerDVTWithHash