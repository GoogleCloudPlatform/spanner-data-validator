package com.google.migration.partitioning;

import com.google.migration.Constants;
import com.google.migration.dto.PartitionRange;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongPartitionRangeListFetcher implements PartitionRangeListFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(LongPartitionRangeListFetcher.class);

  @Override
  public List<PartitionRange> getPartitionRanges(Integer partitionCount) {
    return getPartitionRangesWithCoverage(partitionCount, Constants.PERCENTAGE_CALCULATION_DENOMINATOR);
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
    if(coveragePercent < Constants.PERCENTAGE_CALCULATION_DENOMINATOR) {
      stepSize = (stepSize/Constants.PERCENTAGE_CALCULATION_DENOMINATOR) * coveragePercent;

      if(stepSize <= 0) {
        throw new RuntimeException("Integer step size <= 0!");
      }
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

  @Override
  public List<PartitionRange> getPartitionRangesWithPartitionFilter(String startStr,
      String endStr,
      Integer partitionCount,
      Integer partitionFilterRatio) {

    Long start = Long.parseLong(startStr);
    Long end = Long.parseLong(endStr);
    Long fullRange = end - start;
    Long stepSize = fullRange/partitionCount;

    if(partitionFilterRatio > 0) {
      if(partitionFilterRatio > partitionCount) {
        throw new RuntimeException("PartitionFilterRatio < PartitionCount!");
      }
    }

    ArrayList<PartitionRange> bRanges = new ArrayList<>();

    // Account for first item
    bRanges.add(new PartitionRange(start.toString(), start.toString()));

    Long maxRange = start + 1;
    for(Integer i = 0; i < partitionCount - 1; i++) {

      Long minRange = maxRange;
      maxRange = minRange + stepSize;

      if(partitionFilterRatio > 0 && i % partitionFilterRatio != 0) continue;

      PartitionRange range = new PartitionRange(minRange.toString(), maxRange.toString());

      bRanges.add(range);
    }

    PartitionRange range = new PartitionRange(maxRange.toString(), end.toString());
    bRanges.add(range);

    return bRanges;
  }
} // class LongPartitionRangeListFetcher