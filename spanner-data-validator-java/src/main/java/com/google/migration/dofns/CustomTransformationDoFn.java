package com.google.migration.dofns;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.migration.dto.HashResult;
import com.google.migration.dto.SourceRecord;
import com.google.migration.dto.session.Schema;
import com.google.migration.dto.session.SpannerTable;
import com.google.migration.transform.CustomTransformation;
import com.google.migration.transform.CustomTransformationImplFetcher;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
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

  public static CustomTransformationDoFn create(
      CustomTransformation customTransformer,
      String tableName,
      String shardId,
      Schema schema,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision,
      Integer timestampThresholdKeyIndex
      ) {
    return new AutoValue_CustomTransformationDoFn(customTransformer, tableName, shardId, schema, keyIndex, rangeFieldType, adjustTimestampPrecision, timestampThresholdKeyIndex);
  }

  @Setup
  public void setup() {
    customTransformer = CustomTransformationImplFetcher.getCustomTransformationLogicImpl(customTransformation());
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws InvalidTransformationException {
    SourceRecord sourceRecord = c.element();
    try {
      //if customTransformer is not null, then do the custom transformation.
      if (customTransformer != null) {
        Map<String, Object> sourceRecordMap = getSourceRecordMap(sourceRecord);
        MigrationTransformationResponse migrationTransformationResponse = getCustomTransformationResponse(
            sourceRecordMap, tableName(), shardId());
        if (migrationTransformationResponse.isEventFiltered()) {
          LOG.info("Row was filtered by custom transformer");
          c.output(new HashResult());
        }
        Map<String, Object> transformedCols = migrationTransformationResponse.getResponseRow();
        //There are two possible cases: The column under transformation is an existing one
        //or a new one.
        //If it is an existing one, the sourceRecord needs to be updated in place at the
        //positional value of that column.
        //If it's a new column, add the new columns from custom transformation to the end of the existing sourceRow
        //Sort the transformedCols map by the field name and add it to the existing sourceRecord
        Map<String, Object> sortedTransformedCols = new TreeMap<>(transformedCols);
        for (String transformedColName: sortedTransformedCols.keySet()) {
          boolean fieldExists = false;
          for (int i = 0; i < sourceRecord.length(); i++) {
            if (sourceRecord.getField(i).getFieldName().equals(transformedColName)) {
              sourceRecord.setField(i, transformedColName, getDataTypeFromSchema(transformedColName), sortedTransformedCols.get(transformedColName));
              fieldExists = true;
            }
          }
          if (!fieldExists) sourceRecord.addField(transformedColName, getDataTypeFromSchema(transformedColName) ,sortedTransformedCols.get(transformedColName));
        }
      }
      HashResult hashResult = HashResult.fromSourceRecord(sourceRecord,
          keyIndex(),
          rangeFieldType(),
          adjustTimestampPrecision(),
          timestampThresholdKeyIndex());
      if (hashResult.sha256.isEmpty()) {
        LOG.error("Error while processing element: {} in *********custom transformations*********: ", sourceRecord.toString());
        transformerErrors.inc();
        throw new RuntimeException(String.format("Cannot compute hash for sourceRecord: %s", sourceRecord.toString()));
      }
      c.output(hashResult);
    } catch (Exception e) {
      LOG.error("Error while processing element: {} in *********custom transformations*********: ", sourceRecord.toString(), e);
      transformerErrors.inc();
      throw e;
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
      sourceRecordMap.put(sourceRecord.getField(i).getFieldName(), sourceRecord.getField(i).getFieldValue());
    }
    return sourceRecordMap;
  }

  private String getDataTypeFromSchema(String fieldName) {
    //First get the id of the source table
    String sourceTableId = schema().getSpSchema().entrySet().stream().filter(e -> tableName().equals(e.getValue().getName())).findFirst()
        .map(Entry::getKey).orElseThrow(() -> new RuntimeException("SourceTable not found for tableName: " + tableName()));
    //Get the spannerTable for the id
    SpannerTable spannerTable = schema().getSpSchema().get(sourceTableId);
    if (spannerTable == null) {
      throw new RuntimeException("No spanner table found corresponding to the source table: " + tableName());
    }
    //Fetch the column data type from the spanner table
    String spannerDataType = spannerTable.getColDefs().values().stream().filter(s -> fieldName.equals(s.getName())).findFirst()
        .map(s -> s.getType().getName()).orElseThrow(() -> new RuntimeException(String.format("Field: %s not found in Spanner table: %s", fieldName, spannerTable.getName())));
    return reverseMapSpannerDataTypeToSource(spannerDataType);
  }

  private String reverseMapSpannerDataTypeToSource(String spannerDataType) {
    switch (spannerDataType.toUpperCase()) {
      case "STRING":
        return "VARCHAR";
      case "INT64":
        //It does not really matter if we return "INT" or "BIGINT" here since both will simply
        //get written as string in the string which will represent the record which we take a hash of.
        return "BIGINT";
      case "BOOL":
        return "BOOLEAN";
      case "FLOAT64":
        return "DOUBLE";
      case "TIMESTAMP":
        return "TIMESTAMP";
      case "DATE":
        return "DATE";
      default:
        throw new RuntimeException(String.format("%s is not a supported column data type in custom transformations!", spannerDataType));
    }
  }
}
