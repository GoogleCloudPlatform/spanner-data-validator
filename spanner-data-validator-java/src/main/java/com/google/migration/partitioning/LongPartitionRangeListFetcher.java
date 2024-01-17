package com.google.migration.partitioning;

import com.google.migration.dto.PartitionRange;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongPartitionRangeListFetcher implements PartitionRangeListFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(LongPartitionRangeListFetcher.class);

  @Override
  public List<PartitionRange> getPartitionRanges(Integer partitionCount) {
    return getPartitionRangesWithCoverage(partitionCount, 100);
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(Integer partitionCount,
      Integer coveragePercent) {
    return getPartitionRangesWithCoverage("0",
        String.valueOf(Long.MAX_VALUE),
        partitionCount,
        coveragePercent);
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(String startStr, String endStr,
      Integer partitionCount, Integer coveragePercent) {

    Long start = Long.parseLong(startStr);
    Long end = Long.parseLong(endStr);
    Long fullRange = end - start;
    Long stepSize = fullRange/partitionCount;

    // Simple implementation of "coverage" - just reduce the step size
    if(coveragePercent < 100) {
      stepSize = (stepSize/100) * coveragePercent;
    }

    ArrayList<PartitionRange> bRanges = new ArrayList<>();

    // Account for first item
    bRanges.add(new PartitionRange(start.toString(), start.toString()));

    Long maxRange = start + 1;
    for(Integer i = 0; i < partitionCount - 1; i++) {
      Long minRange = maxRange;
      maxRange = minRange + stepSize;

      PartitionRange range = new PartitionRange(minRange.toString(), maxRange.toString());

      bRanges.add(range);
    }

    PartitionRange range = new PartitionRange(maxRange.toString(), end.toString());
    bRanges.add(range);

    return bRanges;
  }
} // class LongPartitionRangeListFetcher