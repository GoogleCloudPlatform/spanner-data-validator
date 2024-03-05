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

package com.google.migration;

import com.google.migration.dto.HashResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

public class SharedTags {
  // source (jdbc) hash results from initial partitioned query
  public static final TupleTag<HashResult> jdbcTag = new TupleTag<>();
  // spanner hash results from initial partitioned query
  public static final TupleTag<HashResult> spannerTag = new TupleTag<>();
  // matched records by range
  public static final TupleTag<KV<String, Long>> matchedRecordsTag = new TupleTag<KV<String, Long>>(){};
  // source (jdbc) records by range
  public static final TupleTag<KV<String, Long>> sourceRecordsTag =
      new TupleTag<KV<String, Long>>(){};
  // target (spanner) records by range
  public static final TupleTag<KV<String, Long>> targetRecordsTag =
      new TupleTag<KV<String, Long>>(){};
  // unmatched spanner/target records by range
  public static final TupleTag<KV<String, Long>> unmatchedSpannerRecordsTag =
      new TupleTag<KV<String, Long>>(){};
  // unmatched source/jdbc records by range
  public static final TupleTag<KV<String, Long>> unmatchedJDBCRecordsTag =
      new TupleTag<KV<String, Long>>(){};

  // unmatched spanner/target record *values* by range
  public static final TupleTag<HashResult> unmatchedSpannerRecordValuesTag =
      new TupleTag<>() {};
  // unmatched source/jdbc record *values* by range
  public static final TupleTag<HashResult> unmatchedJDBCRecordValuesTag =
      new TupleTag<>() {};

  // count of matched records by range
  public static final TupleTag<Long> matchedRecordCountTag = new TupleTag<>();
  // count of unmatched spanner records by range
  public static final TupleTag<Long> unmatchedSpannerRecordCountTag = new TupleTag<>();
  // count of unmatched jdbc records by range
  public static final TupleTag<Long> unmatchedJDBCRecordCountTag = new TupleTag<>();
  // count of source (jdbc) records by range
  public static final TupleTag<Long> sourceRecordCountTag = new TupleTag<>();
  // count of target (spanner) records by range
  public static final TupleTag<Long> targetRecordCountTag = new TupleTag<>();
} // class SharedTags