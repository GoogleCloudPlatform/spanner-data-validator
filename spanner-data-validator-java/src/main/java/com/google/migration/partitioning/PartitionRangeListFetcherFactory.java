package com.google.migration.partitioning;

import com.google.migration.dto.TableSpec;

public class PartitionRangeListFetcherFactory {
  public static PartitionRangeListFetcher getFetcher(String partitionType) {
    switch(partitionType) {
      case TableSpec.UUID_FIELD_TYPE:
        return new UUIDPartitionRangeListFetcher();
      case TableSpec.INT_FIELD_TYPE:
        return new IntegerPartitionRangeListFetcher();
      case TableSpec.LONG_FIELD_TYPE:
        return new LongPartitionRangeListFetcher();
      default:
        throw new RuntimeException(String.format("Unrecognized partition type: %s", partitionType));
    } // switch
  }
} // class PartitionRangeListFetcherFactory