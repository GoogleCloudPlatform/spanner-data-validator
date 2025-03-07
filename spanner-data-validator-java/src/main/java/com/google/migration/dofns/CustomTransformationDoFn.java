package com.google.migration.dofns;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.migration.dto.HashResult;
import com.google.migration.dto.session.Schema;
import com.google.migration.exceptions.InvalidTransformationException;
import com.google.migration.transform.CustomTransformation;
import com.google.migration.transform.CustomTransformationImplFetcher;
import com.google.migration.transform.ISpannerMigrationTransformer;
import com.google.migration.transform.MigrationTransformationRequest;
import com.google.migration.transform.MigrationTransformationResponse;
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
public abstract class CustomTransformationDoFn extends DoFn<TableRow, HashResult>
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
    TableRow tableRow =  c.element();
    Map<String, Object> tableRowMap = new HashMap<>(tableRow);
    try {
      MigrationTransformationResponse migrationTransformationResponse = getCustomTransformationResponse(tableRowMap, tableName(), shardId());
      if (migrationTransformationResponse.isEventFiltered()) {
        LOG.info("Row was filtered by custom transformer");
        c.output(null);
      }
      Map<String, Object> transformedCols = migrationTransformationResponse.getResponseRow();
      LOG.info("Returned response from the JAR: ", transformedCols.toString());
      c.output(HashResult.fromTableRowMapAndSchema(transformedCols,
          schema(),
          keyIndex(),
          rangeFieldType(),
          adjustTimestampPrecision(),
          timestampThresholdKeyIndex(),
          rangeFieldName(),
           tableName()));
    } catch (Exception e) {
      LOG.error("Error while processing element: ", e);
      transformerErrors.inc();
      c.output(null);
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
}
