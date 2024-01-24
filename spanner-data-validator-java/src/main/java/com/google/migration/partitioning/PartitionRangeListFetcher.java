package com.google.migration.partitioning;

import com.google.migration.dto.PartitionRange;
import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.values.KV;

public interface PartitionRangeListFetcher {
  List<PartitionRange> getPartitionRanges(Integer partitionCount);
  public List<PartitionRange> getPartitionRangesWithCoverage(
      Integer partitionCount,
      BigDecimal coveragePercent);
  public List<PartitionRange> getPartitionRangesWithCoverage(String startStr,
      String endStr,
      Integer partitionCount,
      BigDecimal coveragePercent);
  public List<PartitionRange> getPartitionRangesWithPartitionFilter(String startStr,
      String endStr,
      Integer partitionCount,
      Integer partitionFilterRatio);
} // interface