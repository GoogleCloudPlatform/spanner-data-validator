package com.google.migration.dofns;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.migration.Helpers;
import com.google.migration.dto.ComparerResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DVTProcessorSyncFn extends DoFn<KV<String, Iterable<GenericRecord>>, ComparerResult> {
  private static final Logger LOG = LoggerFactory.getLogger(DVTProcessorSyncFn.class);

  private String projectId;
  private String instanceId;
  private String databaseId;

  private String runName;
  private String query;
  private String schemaStr;

  public DVTProcessorSyncFn(String projectIdIn,
      String instanceIdIn,
      String databaseIdIn,
      String runNameIn,
      String queryIn,
      String schemaStrIn) {
    projectId = projectIdIn;
    instanceId = instanceIdIn;
    databaseId = databaseIdIn;
    runName = runNameIn;
    query = queryIn;
    schemaStr = schemaStrIn;
  }
  @StartBundle
  public void startBundle() {
  }

  void query(DatabaseClient client,
      UUID start,
      UUID end,
      ArrayList<GenericRecord> targetRecords,
      Comparator<GenericRecord> comparator) {

    Statement statement =
        Statement.newBuilder(query)
            .bind("p1")
            .to(start.toString())
            .bind("p2")
            .to(end.toString())
            .build();

    try (ResultSet resultSet =
        client
            .singleUse()
            .executeQuery(statement)) {
      while(resultSet.next()) {
        try {
          GenericRecord avroRecord =
              Helpers.spannerResultSetToGenericRecord(resultSet,
                  schemaStr);

          // we want to maintain a sorted list
          int pos = Collections.binarySearch(targetRecords,
              avroRecord,
              comparator);
          if (pos < 0) {
            targetRecords.add(-pos-1, avroRecord);
          } else {
            // TODO: error handling (don't expect duplicates)
          }
        } catch (Exception ex) {
          LOG.error(String.format("Exception while fetch Spanner records: %s", ex));
        } // try/catch
      } // while

    } // try
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    KV<String, Iterable<GenericRecord>> record = c.element();

    try (Spanner spanner =
        SpannerOptions.newBuilder().setProjectId(projectId).build().getService()) {
      DatabaseClient client =
          spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

      Comparator<GenericRecord> genericRecordComparator =
          Helpers.getGenericRecordComparator();
      ArrayList<GenericRecord> targetRecords = new ArrayList<>();

      // TODO: Create a DTO for this
      String[] value_split = record.getKey().split("\\|");
      KV<UUID, UUID> range = KV.of(UUID.fromString(value_split[0]), UUID.fromString(value_split[1]));

      // TODO: https://stackoverflow.com/questions/75001535/in-gcp-dataflow-apache-beam-python-sdk-is-there-a-time-limit-for-dofn-process
      query(client,
          range.getKey(),
          range.getValue(),
          targetRecords,
          genericRecordComparator);

      ComparerResult result =
          performComparison(c.element().getValue(),
              targetRecords,
              range,
              genericRecordComparator);
      c.output(result);
    } catch (Exception ex) {
      LOG.error(String.format("Error in DVTProcessorSyncFn.processElement: %s", ex));
      LOG.error(String.format("Stack trace: %s", ex.getStackTrace()));
    } // catch
  }

  ComparerResult performComparison(Iterable<GenericRecord> sourceRecords,
      ArrayList<GenericRecord> targetRecords,
      KV<UUID, UUID> range,
      Comparator<GenericRecord> productMetadataComparator) {
    ComparerResult result =
        new ComparerResult(runName,
            range.toString());

    targetRecords.sort(productMetadataComparator);

    result.targetCount = (long)targetRecords.size();
    result.matchCount = 0L;
    result.sourceConflictCount = 0L;
    result.targetConflictCount = 0L;

    result.sourceCount = 0L;
    for(GenericRecord source: sourceRecords) {
      result.sourceCount++;

      int searchResult = Collections.binarySearch(targetRecords,
          source,
          productMetadataComparator);

      if(searchResult >= 0) {
      } else {
        result.sourceConflictCount++;
      }
    } // for

    return result;
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext c) {
  }
} // class DVTProcessorSyncFn