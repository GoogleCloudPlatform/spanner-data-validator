package com.google.migration.partitioning;

import com.google.migration.dto.PartitionRange;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringPartitionListFetcher implements PartitionRangeListFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(StringPartitionListFetcher.class);
  @Override
  public List<PartitionRange> getPartitionRanges(Integer partitionCount) {
    throw new NotImplementedException("getPartitionRanges");
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(Integer partitionCount,
      BigDecimal coveragePercent) {
    throw new NotImplementedException("getPartitionRangesWithCoverage");
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(String startStr,
      String endStr,
      Integer partitionCount,
      BigDecimal coveragePercent) {
    if(partitionCount != 1) {
      throw new IllegalArgumentException("Partition count must be 1 for String");
    }

    if(coveragePercent.compareTo(BigDecimal.ONE) != 0) {
      throw new IllegalArgumentException("Coverage percent must be 1 (aka 100%) for String");
    }

    if(startStr.compareTo(endStr) > 0) {
      throw new IllegalArgumentException("Start range must <= end range for String");
    }

    ArrayList<PartitionRange> partitionRanges = new ArrayList<>();
    partitionRanges.add(new PartitionRange(startStr, endStr));

    return partitionRanges;
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithPartitionFilter(String startStr, String endStr,
      Integer partitionCount, Integer partitionFilterRatio) {
    throw new NotImplementedException("getPartitionRangesWithPartitionFilter");
  }
} // class StringPartitionListFetcher