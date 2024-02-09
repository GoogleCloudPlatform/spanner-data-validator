package com.google.migration.partitioning;

import com.google.migration.dto.TableSpec;
import java.util.Locale;

public class PartitionRangeListFetcherFactory {

  public static PartitionRangeListFetcher getFetcher(String partitionType) {
    partitionType = partitionType.toUpperCase();

    switch(partitionType) {
      case TableSpec.UUID_FIELD_TYPE:
        return new UUIDPartitionRangeListFetcher();
      case TableSpec.INT_FIELD_TYPE:
        return new IntegerPartitionRangeListFetcher();
      case TableSpec.LONG_FIELD_TYPE:
        return new LongPartitionRangeListFetcher();
      case TableSpec.TIMESTAMP_FIELD_TYPE:
        return new TimestampPartitionRangeListFetcher();
      default:
        throw new RuntimeException(String.format("Unrecognized partition type: %s", partitionType));
    } // switch
  }
} // class PartitionRangeListFetcherFactory