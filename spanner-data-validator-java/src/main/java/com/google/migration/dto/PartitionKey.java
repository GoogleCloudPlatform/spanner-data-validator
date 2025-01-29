package com.google.migration.dto;

public class PartitionKey {

  private final String partitionKeyColId;

  private final String partitionKeyColDataType;

  public PartitionKey(String partitionKeyColId, String partitionKeyColDataType) {
    this.partitionKeyColId = partitionKeyColId;
    this.partitionKeyColDataType = partitionKeyColDataType;
  }

  public String getPartitionKeyColId() {
    return partitionKeyColId;
  }

  public String getPartitionKeyColDataType() {
    return partitionKeyColDataType;
  }
}
