package com.google.migration.partitioning;

import com.google.migration.dto.PartitionRange;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UUIDPartitionRangeListFetcher implements PartitionRangeListFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(UUIDPartitionRangeListFetcher.class);
  @Override
  public List<PartitionRange> getPartitionRanges(Integer partitionCount) {
    return getPartitionRangesWithCoverage(partitionCount,
        BigDecimal.ONE);
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(
      Integer partitionCount,
      BigDecimal coveragePercent) {
    return getPartitionRangesWithCoverage("00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        partitionCount,
        coveragePercent);
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(String startStr,
      String endStr,
      Integer partitionCount,
      BigDecimal coveragePercent) {
    UUID start = UUID.fromString(startStr);
    UUID end = UUID.fromString(endStr);

    if(coveragePercent.compareTo(BigDecimal.ONE) > 0) {
      throw new IllegalArgumentException("Coverage percent must be <= 1");
    }

    Boolean partialCoverage = (coveragePercent.compareTo(BigDecimal.ONE) < 0);

    // UUID max
    BigInteger uuidMax = UUIDHelpers.uuidToBigInt(end);
    BigInteger uuidMin = UUIDHelpers.uuidToBigInt(start);
    BigInteger fullRange = uuidMax.subtract(uuidMin);
    BigInteger stepSize = fullRange.divide(BigInteger.valueOf(partitionCount.intValue()));
    BigInteger constrainedStepSize = stepSize;

    // Simple implementation of "coverage" - just reduce the step size
    if(partialCoverage) {
      BigDecimal constrainedStepSizeDecimal = new BigDecimal(constrainedStepSize);
      constrainedStepSize = constrainedStepSizeDecimal.multiply(coveragePercent).toBigInteger();

      LOG.info(String.format("Step size: %s in "
          + "UUIDPartitionRangeListFetcher.getPartitionRangesWithCoverage", stepSize));

      if(constrainedStepSize.compareTo(BigInteger.ONE) <= 0) {
        throw new RuntimeException("UUID constrainedStepSize size <= 0!");
      }
    }

    ArrayList<PartitionRange> bRanges = new ArrayList<>();

    if(partitionCount <= 0) {
      throw new IllegalArgumentException("Partition count must be > 0");
    } else if(partitionCount == 1) {
      String calculatedEndRangeStr = endStr;
      if (partialCoverage) {
        BigInteger calculatedEndRange = UUIDHelpers.uuidToBigInt(start).add(constrainedStepSize);
        calculatedEndRangeStr = UUIDHelpers.bigIntToUUID(calculatedEndRange).toString();
      }

      PartitionRange range = new PartitionRange(startStr, calculatedEndRangeStr);
      bRanges.add(range);
    } else {
      BigInteger maxRange = uuidMin.subtract(BigInteger.ONE);
      for (Integer i = 0; i < partitionCount - 1; i++) {
        BigInteger minRange = maxRange.add(BigInteger.ONE);
        maxRange = minRange.add(constrainedStepSize).subtract(BigInteger.ONE);

        PartitionRange range = new PartitionRange(UUIDHelpers.bigIntToUUID(minRange).toString(),
            UUIDHelpers.bigIntToUUID(maxRange).toString());

        bRanges.add(range);

        maxRange = minRange.add(stepSize).subtract(BigInteger.ONE);
      }

      BigInteger calculatedEndRange = maxRange.add(constrainedStepSize);
      if (!partialCoverage) {
        calculatedEndRange = uuidMax;
      }
      PartitionRange range = new PartitionRange(UUIDHelpers.bigIntToUUID(maxRange).toString(),
          UUIDHelpers.bigIntToUUID(calculatedEndRange).toString());
      bRanges.add(range);
    }

    return bRanges;
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithPartitionFilter(String startStr,
      String endStr,
      Integer partitionCount,
      Integer partitionFilterRatio) {
    UUID start = UUID.fromString(startStr);
    UUID end = UUID.fromString(endStr);

    // UUID max
    BigInteger uuidMax = UUIDHelpers.uuidToBigInt(end);
    BigInteger uuidMin = UUIDHelpers.uuidToBigInt(start);
    BigInteger fullRange = uuidMax.subtract(uuidMin);
    BigInteger stepSize = fullRange.divide(BigInteger.valueOf(partitionCount.intValue()));

    if(partitionFilterRatio > 0) {
      if(partitionFilterRatio > partitionCount) {
        throw new RuntimeException("PartitionFilterRatio < PartitionCount!");
      }
    }

    ArrayList<PartitionRange> bRanges = new ArrayList<>();

    // Account for first UUID
    bRanges.add(new PartitionRange(start.toString(), start.toString()));

    BigInteger maxRange = uuidMin.add(BigInteger.ONE);
    for(Integer i = 0; i < partitionCount - 1; i++) {

      BigInteger minRange = maxRange;
      maxRange = minRange.add(stepSize);

      if(partitionFilterRatio > 0 && i % partitionFilterRatio != 0) continue;

      PartitionRange range = new PartitionRange(UUIDHelpers.bigIntToUUID(minRange).toString(),
          UUIDHelpers.bigIntToUUID(maxRange).toString());

      bRanges.add(range);
    }

    PartitionRange range = new PartitionRange(UUIDHelpers.bigIntToUUID(maxRange).toString(),
        UUIDHelpers.bigIntToUUID(uuidMax).toString());
    bRanges.add(range);

    return bRanges;
  }
} // class UUIDPartitionRangeListFetcher