package com.google.migration.dto;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(AvroCoder.class)
public class PartitionRange {

  public String getStartRange() {
    return startRange;
  }

  public void setStartRange(String startRange) {
    this.startRange = startRange;
  }

  String startRange;

  public String getEndRange() {
    return endRange;
  }

  public void setEndRange(String endRange) {
    this.endRange = endRange;
  }

  String endRange;

  public PartitionRange() {
  }

  public PartitionRange(String startRange, String endRange) {
    this.startRange = startRange;
    this.endRange = endRange;
  }
} // class PartitionRange