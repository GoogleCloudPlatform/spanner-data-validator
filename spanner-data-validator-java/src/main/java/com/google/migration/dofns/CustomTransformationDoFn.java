package com.google.migration.dofns;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.migration.dto.HashResult;
import com.google.migration.dto.SourceRecord;
import com.google.migration.dto.session.Schema;
import com.google.migration.dto.session.SourceTable;
import com.google.migration.transform.CustomTransformation;
import com.google.migration.transform.CustomTransformationImplFetcher;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class CustomTransformationDoFn extends DoFn<SourceRecord, HashResult>
  implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(CustomTransformationDoFn.class);

  private ISpannerMigrationTransformer customTransformer;

  private final Distribution applyCustomTransformationResponseTimeMetric =
      Metrics.distribution(
          CustomTransformationDoFn.class, "apply_custom_transformation_impl_latency_ms");

  private final Counter transformerErrors =
      Metrics.counter(CustomTransformationDoFn.class, "transformer_errors");

  public void setCustomTransformer(
      ISpannerMigrationTransformer customTransformer) {
    this.customTransformer = customTransformer;
  }

  @Nullable
  public abstract CustomTransformation customTransformation();

  @Nullable
  public abstract String tableName();

  @Nullable
  public abstract String shardId();

  @Nullable
  public abstract Schema schema();

  @Nullable
  public abstract Integer keyIndex();

  @Nullable
  public abstract String rangeFieldType();

  @Nullable
  public abstract Boolean adjustTimestampPrecision();

  @Nullable
  public abstract Integer timestampThresholdKeyIndex();

  @Nullable
  public abstract String rangeFieldName();

  public static CustomTransformationDoFn create(
      CustomTransformation customTransformer,
      String tableName,
      String shardId,
      Schema schema,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision,
      Integer timestampThresholdKeyIndex,
      String rangeFieldName
      ) {
    return new AutoValue_CustomTransformationDoFn(customTransformer, tableName, shardId, schema, keyIndex, rangeFieldType, adjustTimestampPrecision, timestampThresholdKeyIndex, rangeFieldName);
  }

  @Setup
  public void setup() {
    customTransformer = CustomTransformationImplFetcher.getCustomTransformationLogicImpl(customTransformation());
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    SourceRecord sourceRecord = c.element();
    LOG.info("Data read from JDBC: {}", sourceRecord.toString());
    Map<String, Object> sourceRecordMap = getSourceRecordMap(sourceRecord);
    try {
      MigrationTransformationResponse migrationTransformationResponse = getCustomTransformationResponse(
          sourceRecordMap, tableName(), shardId());
      if (migrationTransformationResponse.isEventFiltered()) {
        LOG.info("Row was filtered by custom transformer");
        c.output(new HashResult());
      }
      Map<String, Object> transformedCols = migrationTransformationResponse.getResponseRow();
      LOG.info("Returned response from the JAR: {}", transformedCols.toString());
      //Add the new columns from custom transformation to the end of the existing sourceRow
      for (Map.Entry<String, Object> entry : transformedCols.entrySet()) {
        sourceRecord.addField(entry.getKey(), getDataTypeFromSchema(entry.getKey(), tableName(), schema()) ,entry.getValue());
      }
      LOG.info("Response sent for hashing: {}", sourceRecord.toString());
      HashResult hashResult = HashResult.fromSourceRecord(sourceRecord,
          keyIndex(),
          rangeFieldType(),
          adjustTimestampPrecision(),
          timestampThresholdKeyIndex());
      c.output(hashResult);
    } catch (Exception e) {
      LOG.error("Error while processing element: ", e);
      transformerErrors.inc();
      c.output(new HashResult());
    }
  }

  @VisibleForTesting
  private MigrationTransformationResponse getCustomTransformationResponse(
      Map<String, Object> sourceRecord, String tableName, String shardId)
      throws InvalidTransformationException {

    org.joda.time.Instant startTimestamp = org.joda.time.Instant.now();
    MigrationTransformationRequest migrationTransformationRequest =
        new MigrationTransformationRequest(tableName, sourceRecord, shardId, "INSERT");
    LOG.debug(
        "using migration transformation request {} for table {}",
        migrationTransformationRequest,
        tableName);
    MigrationTransformationResponse migrationTransformationResponse;
    try {
      migrationTransformationResponse =
          customTransformer.toSpannerRow(migrationTransformationRequest);
    } finally {
      org.joda.time.Instant endTimestamp = org.joda.time.Instant.now();
      applyCustomTransformationResponseTimeMetric.update(
          new Duration(startTimestamp, endTimestamp).getMillis());
    }
    LOG.debug(
        "Got migration transformation response {} for table {}",
        migrationTransformationResponse,
        tableName);
    return migrationTransformationResponse;
  }

  private Map<String, Object> getSourceRecordMap(SourceRecord sourceRecord) {
    Map<String, Object> sourceRecordMap = new HashMap<>();
    for (int i = 0; i < sourceRecord.length(); i++) {
      sourceRecordMap.put(sourceRecord.getField(i).getFieldName(), sourceRecord.getField(i));
    }
    return sourceRecordMap;
  }

  private String getDataTypeFromSchema(String fieldName, String tableName, Schema schema) {
    SourceTable sourceTable = schema.getSrcSchema().entrySet().stream().filter((e) -> e.getValue().getName().equals(tableName)).findFirst().get().getValue();
    if (sourceTable == null) {
      throw new RuntimeException("SourceTable not found for tableName: " + tableName);
    }
    return sourceTable.getColDefs().values().stream().filter(s -> fieldName.equals(s.getName())).findFirst().get().getType().getName();
  }
}
