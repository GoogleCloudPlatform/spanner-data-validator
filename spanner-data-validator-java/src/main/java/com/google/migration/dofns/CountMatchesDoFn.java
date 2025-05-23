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

import com.google.migration.Helpers;
import com.google.migration.dto.HashResult;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountMatchesDoFn extends DoFn<KV<String, CoGbkResult>, KV<String, Long>> {
  private static final Logger LOG = LoggerFactory.getLogger(CountMatchesDoFn.class);
  private long timestampThreshold;
  private Integer timestampThresholdDeltaInMins = 0;
  private boolean timestampThresholdInEffect;
  private long timestampFilterStart = 0;
  private long timestampFilterEnd = 0;

  public CountMatchesDoFn(long timestampThresholdIn, Integer timestampThresholdDeltaInMinsIn) {
    timestampThreshold = timestampThresholdIn;
    timestampThresholdInEffect = timestampThreshold > 0;
    timestampThresholdDeltaInMins = timestampThresholdDeltaInMinsIn;
    LOG.info(String.format("Timestamp threshold in effect: %s", timestampThresholdInEffect));
    LOG.info(String.format("***Timestamp threshold delta in mins: %d", timestampThresholdDeltaInMins));

    if(timestampThresholdInEffect) {
      timestampFilterStart = Math.min(timestampThreshold + ((long)timestampThresholdDeltaInMinsIn * 60 * 1000), timestampThreshold);
      if(timestampThresholdDeltaInMins != 0) {
        timestampFilterEnd = Math.max(timestampThreshold + ((long)timestampThresholdDeltaInMinsIn * 60 * 1000), timestampThreshold);
      } else {
        timestampFilterEnd = Instant
            .ofEpochMilli(timestampFilterStart)
            .plus(10000, ChronoUnit.DAYS)
            .toEpochMilli(); // add 10000 days
      }
    }
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

    boolean emitOutput = true;

    if (spannerRecord != null && jdbcRecord != null) {
      if(timestampThresholdInEffect) {
        emitOutput = spannerRecord.timestampThresholdValue >= timestampFilterStart &&
            spannerRecord.timestampThresholdValue <= timestampFilterEnd;

        // Helpers.printTimestampThresholdInfo(spannerRecord.timestampThresholdValue,
        //     timestampFilterStart,
        //     timestampFilterEnd);
      }

      if(emitOutput) {
        out.get(matchedRecordsTag).output(KV.of(spannerRecord.range, 1L));
        out.get(sourceRecordsTag).output(KV.of(spannerRecord.range, 1L));
        out.get(targetRecordsTag).output(KV.of(spannerRecord.range, 1L));
      }
    } else if(spannerRecord != null) {
      if(timestampThresholdInEffect) {
        emitOutput = spannerRecord.timestampThresholdValue >= timestampFilterStart &&
            spannerRecord.timestampThresholdValue <= timestampFilterEnd;
      }

      if(emitOutput) {
        out.get(unmatchedSpannerRecordsTag).output(KV.of(spannerRecord.range, 1L));
        out.get(targetRecordsTag).output(KV.of(spannerRecord.range, 1L));
        out.get(unmatchedSpannerRecordValuesTag).output(spannerRecord);
      }
    } else if(jdbcRecord != null) {
      if(timestampThresholdInEffect) {
        emitOutput = jdbcRecord.timestampThresholdValue >= timestampFilterStart &&
            jdbcRecord.timestampThresholdValue <= timestampFilterEnd;
      }

      if(emitOutput) {
        out.get(unmatchedJDBCRecordsTag).output(KV.of(jdbcRecord.range, 1L));
        out.get(sourceRecordsTag).output(KV.of(jdbcRecord.range, 1L));
        out.get(unmatchedJDBCRecordValuesTag).output(jdbcRecord);
      }
    } // if/else
  }
} // class CountMatchesDoFn
