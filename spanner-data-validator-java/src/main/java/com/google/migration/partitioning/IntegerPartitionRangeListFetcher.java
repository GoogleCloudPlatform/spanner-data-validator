package com.google.migration.partitioning;

import com.google.migration.dto.PartitionRange;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegerPartitionRangeListFetcher implements PartitionRangeListFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(IntegerPartitionRangeListFetcher.class);

  @Override
  public List<PartitionRange> getPartitionRanges(Integer partitionCount) {
    return getPartitionRangesWithCoverage(partitionCount,
        BigDecimal.ONE);
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(Integer partitionCount,
      BigDecimal coveragePercent) {
    return getPartitionRangesWithCoverage("0",
        String.valueOf(Integer.MAX_VALUE),
        partitionCount,
        coveragePercent);
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(String startStr,
      String endStr,
      Integer partitionCount,
      BigDecimal coveragePercent) {

    Integer start = Integer.parseInt(startStr);
    Integer end = Integer.parseInt(endStr);
    Integer fullRange = end - start;
    Integer stepSize = fullRange/partitionCount;
    Integer constrainedStepSize = stepSize;

    if(coveragePercent.compareTo(BigDecimal.ONE) > 0) {
      throw new IllegalArgumentException("Coverage percent must be <= 1");
    }

    Boolean partialCoverage = (coveragePercent.compareTo(BigDecimal.ONE) < 0);

    // Simple implementation of "coverage" - just reduce the step size
    if(partialCoverage) {
      constrainedStepSize = BigDecimal.valueOf(stepSize).multiply(coveragePercent).intValue();

      LOG.info(String.format("Constrained step size: %d in "
          + "IntegerPartitionRangeListFetcher.getPartitionRangesWithCoverage", constrainedStepSize));

      if(constrainedStepSize <= 0) {
        throw new RuntimeException("Integer step size <= 0!");
      }
    }

    ArrayList<PartitionRange> bRanges = new ArrayList<>();

    if(partitionCount <= 0) {
      throw new IllegalArgumentException("Partition count must be > 0");
    } else if (partitionCount == 1) {
      String calculatedEndRangeStr = endStr;

      if(partialCoverage) {
        calculatedEndRangeStr = String.valueOf(start + constrainedStepSize);
      }

      PartitionRange range = new PartitionRange(startStr, calculatedEndRangeStr);
      bRanges.add(range);
    } else {
      Integer maxRange = start - 1;
      for (Integer i = 0; i < partitionCount - 1; i++) {
        Integer minRange = maxRange + 1;
        maxRange = minRange + constrainedStepSize - 1;

        PartitionRange range = new PartitionRange(minRange.toString(), maxRange.toString());

        bRanges.add(range);

        maxRange = minRange + stepSize - 1;
      }

      Integer calculatedEndRange = maxRange + constrainedStepSize;
      if (!partialCoverage) {
        calculatedEndRange = end;
      }
      PartitionRange range = new PartitionRange(maxRange.toString(), calculatedEndRange.toString());
      bRanges.add(range);
    }

    return bRanges;
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithPartitionFilter(String startStr,
      String endStr,
      Integer partitionCount,
      Integer partitionFilterRatio) {

    Integer start = Integer.parseInt(startStr);
    Integer end = Integer.parseInt(endStr);
    Integer fullRange = end - start;
    Integer stepSize = fullRange/partitionCount;

    if(partitionFilterRatio > 0) {
      if(partitionFilterRatio < partitionCount) {
        throw new RuntimeException("PartitionFilterRatio < PartitionCount!");
      }
    }

    ArrayList<PartitionRange> bRanges = new ArrayList<>();

    // Account for first item
    bRanges.add(new PartitionRange(start.toString(), start.toString()));

    Integer maxRange = start + 1;
    for(Integer i = 0; i < partitionCount - 1; i++) {

      Integer minRange = maxRange;
      maxRange = minRange + stepSize;

      if(partitionFilterRatio > 0 && i % partitionFilterRatio != 0) continue;

      PartitionRange range = new PartitionRange(minRange.toString(), maxRange.toString());

      bRanges.add(range);
    }

    PartitionRange range = new PartitionRange(maxRange.toString(), end.toString());
    bRanges.add(range);

    return bRanges;
  }
} // class IntegerPartitionRangeListFetcher