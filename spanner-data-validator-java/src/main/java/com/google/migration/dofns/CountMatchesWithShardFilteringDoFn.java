package com.google.migration.dofns;

import static com.google.migration.SharedTags.jdbcTag;
import static com.google.migration.SharedTags.matchedRecordsTag;
import static com.google.migration.SharedTags.sourceRecordsTag;
import static com.google.migration.SharedTags.spannerTag;
import static com.google.migration.SharedTags.targetRecordsTag;
import static com.google.migration.SharedTags.unmatchedJDBCRecordValuesTag;
import static com.google.migration.SharedTags.unmatchedJDBCRecordsTag;
import static com.google.migration.SharedTags.unmatchedSpannerRecordValuesTag;
import static com.google.migration.SharedTags.unmatchedSpannerRecordsTag;

import com.google.migration.dto.HashResult;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountMatchesWithShardFilteringDoFn extends
    DoFn<KV<String, CoGbkResult>, KV<String, Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(CountMatchesWithShardFilteringDoFn.class);

  private Boolean enableVerboseLogging = false;
  private List<String> shardsToInclude = null;
  private String runName;

  String spannerDVTRunNamespace = String.format("SpannerDVT-%s", runName);
  Counter matchedRecordsCounter = Metrics.counter(spannerDVTRunNamespace, "matchedrecords");
  Counter unmatchedJDBCRecordsCounter = Metrics.counter(spannerDVTRunNamespace, "unmatchedjdbcrecords");
  Counter unmatchedSpannerRecordsCounter = Metrics.counter(spannerDVTRunNamespace, "unmatchedspannerrecords");
  Counter sourceRecordCounter = Metrics.counter(spannerDVTRunNamespace, "sourcerecords");
  Counter targetRecordCounter = Metrics.counter(spannerDVTRunNamespace, "targetrecords");

  public CountMatchesWithShardFilteringDoFn() {
  }

  public CountMatchesWithShardFilteringDoFn(List<String> shardsToIncludeIn,
      Boolean enableVerboseLoggingIn,
      String runNameIn) {
    shardsToInclude = shardsToIncludeIn;
    enableVerboseLogging = enableVerboseLoggingIn;
    runName = runNameIn;
  }

  @ProcessElement
  public void processElement(ProcessContext c, MultiOutputReceiver out) {
    KV<String, CoGbkResult> e = c.element();
    Iterable<HashResult> jdbcRecords = e.getValue().getAll(jdbcTag);
    Iterable<HashResult> spannerRecords = e.getValue().getAll(spannerTag);

    HashResult jdbcRecord = null, spannerRecord = null;
    if (jdbcRecords.iterator().hasNext()) {
      jdbcRecord = jdbcRecords.iterator().next();
    }

    if (spannerRecords.iterator().hasNext()) {
      spannerRecord = spannerRecords.iterator().next();
    }

    if(spannerRecord != null) {
      if(shardsToInclude != null && shardsToInclude.size() > 0) {
        if(!shardsToInclude.contains(spannerRecord.logicalShardId)) {

          if(enableVerboseLogging) {
            LOG.warn("Skipping record w/ logical shard id: {}", spannerRecord.logicalShardId);
            LOG.warn("Shards to include: {}", String.join(",", shardsToInclude));
          }
          return;
        }
      }
    }

    if (spannerRecord != null && jdbcRecord != null) {
      matchedRecordsCounter.inc();
      sourceRecordCounter.inc();
      targetRecordCounter.inc();

      out.get(matchedRecordsTag).output(KV.of(spannerRecord.range, 1L));
      out.get(sourceRecordsTag).output(KV.of(spannerRecord.range, 1L));
      out.get(targetRecordsTag).output(KV.of(spannerRecord.range, 1L));
    } else if(spannerRecord != null) {
      unmatchedSpannerRecordsCounter.inc();
      targetRecordCounter.inc();

      out.get(unmatchedSpannerRecordsTag).output(KV.of(spannerRecord.range, 1L));
      out.get(targetRecordsTag).output(KV.of(spannerRecord.range, 1L));
      out.get(unmatchedSpannerRecordValuesTag).output(spannerRecord);
    } else if(jdbcRecord != null) {
      unmatchedJDBCRecordsCounter.inc();
      sourceRecordCounter.inc();

      out.get(unmatchedJDBCRecordsTag).output(KV.of(jdbcRecord.range, 1L));
      out.get(sourceRecordsTag).output(KV.of(jdbcRecord.range, 1L));
      out.get(unmatchedJDBCRecordValuesTag).output(jdbcRecord);
    } // if/else
  }
} // class CountMatchesWithShardFilteringDoFn
