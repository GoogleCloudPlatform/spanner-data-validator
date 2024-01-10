package com.google.migration.partitioning;

import com.google.migration.dto.TableSpec;

public class PartitionRangeListFetcherFactory {
  public static PartitionRangeListFetcher getFetcher(String partitionType) {
    switch(partitionType) {
      case TableSpec.UUID_FIELD_TYPE:
        return new UUIDPartitionRangeListFetcher();
      default:
        throw new RuntimeException(String.format("Unrecognized partition type: %s", partitionType));
    } // switch
  }
} // class PartitionRangeListFetcherFactory