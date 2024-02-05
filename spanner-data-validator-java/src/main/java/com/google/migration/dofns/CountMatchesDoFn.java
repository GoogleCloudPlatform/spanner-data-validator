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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;

public class CountMatchesDoFn extends DoFn<KV<String, CoGbkResult>, KV<String, Long>> {
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
} // class CountMatchesDoFn