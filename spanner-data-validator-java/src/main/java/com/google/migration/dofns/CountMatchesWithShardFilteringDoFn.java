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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountMatchesWithShardFilteringDoFn extends
    DoFn<KV<String, CoGbkResult>, KV<String, Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(CountMatchesWithShardFilteringDoFn.class);

  private List<String> shardsToInclude = null;
  public CountMatchesWithShardFilteringDoFn() {
  }

  public CountMatchesWithShardFilteringDoFn(List<String> shardsToIncludeIn) {
    shardsToInclude = shardsToIncludeIn;
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
        if(!shardsToInclude.contains(spannerRecord.getLogicalShardId())) {
          return;
        }
      }
    }

    if (spannerRecord != null && jdbcRecord != null) {
      out.get(matchedRecordsTag).output(KV.of(spannerRecord.range, 1L));
      out.get(sourceRecordsTag).output(KV.of(spannerRecord.range, 1L));
      out.get(targetRecordsTag).output(KV.of(spannerRecord.range, 1L));
    } else if(spannerRecord != null) {
      out.get(unmatchedSpannerRecordsTag).output(KV.of(spannerRecord.range, 1L));
      out.get(targetRecordsTag).output(KV.of(spannerRecord.range, 1L));
      out.get(unmatchedSpannerRecordValuesTag).output(spannerRecord);
    } else if(jdbcRecord != null) {
      out.get(unmatchedJDBCRecordsTag).output(KV.of(jdbcRecord.range, 1L));
      out.get(sourceRecordsTag).output(KV.of(jdbcRecord.range, 1L));
      out.get(unmatchedJDBCRecordValuesTag).output(jdbcRecord);
    } // if/else
  }
} // class CountMatchesWithShardFilteringDoFn
