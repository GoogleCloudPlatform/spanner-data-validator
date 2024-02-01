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