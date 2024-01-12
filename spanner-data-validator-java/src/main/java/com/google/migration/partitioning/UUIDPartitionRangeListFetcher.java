package com.google.migration.partitioning;

import com.google.migration.dto.PartitionRange;
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
    return getPartitionRangesWithCoverage(partitionCount, 100);
  }

  public List<PartitionRange> getPartitionRangesWithCoverage(
      Integer partitionCount,
      Integer coveragePercent) {
    return getPartitionRangesWithCoverage("00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        partitionCount,
        coveragePercent);
  }

  public List<PartitionRange> getPartitionRangesWithCoverage(String startStr,
      String endStr,
      Integer partitionCount,
      Integer coveragePercent) {
    UUID start = UUID.fromString(startStr);
    UUID end = UUID.fromString(endStr);

    // UUID max
    BigInteger uuidMax = UUIDHelpers.uuidToBigInt(end);
    BigInteger uuidMin = UUIDHelpers.uuidToBigInt(start);
    BigInteger fullRange = uuidMax.subtract(uuidMin);
    BigInteger stepSize = fullRange.divide(BigInteger.valueOf(partitionCount.intValue()));

    // Simple implementation of "coverage" - just reduce the step size
    if(coveragePercent < 100) {
      stepSize = stepSize
          .divide(BigInteger.valueOf(100))
          .multiply(BigInteger.valueOf(coveragePercent));
    }

    ArrayList<PartitionRange> bRanges = new ArrayList<>();

    // Account for first UUID
    bRanges.add(new PartitionRange(start.toString(), start.toString()));

    BigInteger maxRange = uuidMin.add(BigInteger.ONE);
    for(Integer i = 0; i < partitionCount - 1; i++) {
      BigInteger minRange = maxRange;
      maxRange = minRange.add(stepSize);

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