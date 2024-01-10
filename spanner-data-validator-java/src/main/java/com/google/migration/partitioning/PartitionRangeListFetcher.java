package com.google.migration.partitioning;

import com.google.migration.dto.PartitionRange;
import java.util.List;
import org.apache.beam.sdk.values.KV;

public interface PartitionRangeListFetcher {
  List<PartitionRange> getPartitionRanges(Integer partitionCount);
  public List<PartitionRange> getPartitionRangesWithCoverage(
      Integer partitionCount,
      Integer coveragePercent);
  public List<PartitionRange> getPartitionRangesWithCoverage(String startStr,
      String endStr,
      Integer partitionCount,
      Integer coveragePercent);
} // interface