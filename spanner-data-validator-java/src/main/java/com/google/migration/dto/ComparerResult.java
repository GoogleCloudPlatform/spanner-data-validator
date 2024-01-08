package com.google.migration.dto;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(AvroCoder.class)
public class ComparerResult {
  public String runName;
  public String range;
  public Long sourceCount;
  public Long targetCount;
  public Long matchCount;

  public Long sourceConflictCount;
  public Long targetConflictCount;

  public ComparerResult() {}

  public ComparerResult(String runName, String range) {
    this.runName = runName;
    this.range = range;
  }
} // class ComparerResult