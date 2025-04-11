package com.google.migration.dto;

public class PartitionKey {

  private final String partitionKeyColId;

  private final String partitionKeyColDataType;

  private final String partitionKeyMinValue;

  private final String partitionKeyMaxValue;

  public PartitionKey(String partitionKeyColId, String partitionKeyColDataType,
      String partitionKeyMinValue,
      String partitionKeyMaxValue) {
    this.partitionKeyColId = partitionKeyColId;
    this.partitionKeyColDataType = partitionKeyColDataType;
    this.partitionKeyMinValue = partitionKeyMinValue;
    this.partitionKeyMaxValue = partitionKeyMaxValue;
  }

  public String getPartitionKeyColId() {
    return partitionKeyColId;
  }

  public String getPartitionKeyColDataType() {
    return partitionKeyColDataType;
  }

  public String getPartitionKeyMaxValue() {
    return partitionKeyMaxValue;
  }

  public String getPartitionKeyMinValue() {
    return partitionKeyMinValue;
  }
}
