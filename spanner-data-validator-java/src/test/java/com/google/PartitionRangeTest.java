package com.google;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.migration.Helpers;
import com.google.migration.dto.PartitionRange;
import com.google.migration.dto.TableSpec;
import com.google.migration.partitioning.PartitionRangeListFetcher;
import com.google.migration.partitioning.PartitionRangeListFetcherFactory;
import com.google.migration.partitioning.UUIDHelpers;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import org.junit.Test;

public class PartitionRangeTest {

  @Test
  public void longPartitionRangeTest() {
    String fieldType = TableSpec.LONG_FIELD_TYPE;

    // fetch w/ partition filter ratio
    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    Integer partitionCount = 100;

    // fetch w/ partition 50% coverage
    List<PartitionRange> pRanges50 = fetcher.getPartitionRangesWithCoverage(String.valueOf(0L),
        String.valueOf(Long.MAX_VALUE),
        partitionCount,
        BigDecimal.valueOf(0.5));

    assertEquals(pRanges50.size(), (long)partitionCount);
    assertEquals("0", pRanges50.get(0).getStartRange());
    assertNotEquals(String.valueOf(Long.MAX_VALUE), pRanges50.get(partitionCount-1).getEndRange());

    Helpers.printPartitionRanges(pRanges50, "TestTable50");

    // fetch w/ partition full coverage
    List<PartitionRange> pRangesFull = fetcher.getPartitionRangesWithCoverage(String.valueOf(0L),
        String.valueOf(Long.MAX_VALUE),
        partitionCount,
        BigDecimal.ONE);

    assertEquals(pRangesFull.size(), (long)partitionCount);
    assertEquals("0", pRangesFull.get(0).getStartRange());
    assertEquals(String.valueOf(Long.MAX_VALUE), pRangesFull.get(partitionCount-1).getEndRange());

    String range1EndStr = pRangesFull.get(1).getEndRange();
    String range2StartStr = pRangesFull.get(2).getStartRange();
    Long range1End = Long.parseLong(range1EndStr);
    Long range2Start = Long.parseLong(range2StartStr);
    assertTrue(range1End + 1 == range2Start);

    Helpers.printPartitionRanges(pRangesFull, "TestTableFull");

    assertEquals(pRanges50.get(partitionCount-1).getStartRange(),
        pRangesFull.get(partitionCount-1).getStartRange());
  }

  @Test
  public void int32PartitionRangeTest() {
    String fieldType = TableSpec.INT_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    Integer partitionCount = 100;

    // fetch w/ partition 50% coverage
    List<PartitionRange> pRanges50 = fetcher.getPartitionRangesWithCoverage(String.valueOf(0),
        String.valueOf(Integer.MAX_VALUE),
        partitionCount,
        BigDecimal.valueOf(0.5));

    assertEquals(pRanges50.size(), (long)partitionCount);
    assertEquals("0", pRanges50.get(0).getStartRange());
    assertNotEquals(String.valueOf(Integer.MAX_VALUE), pRanges50.get(partitionCount-1).getEndRange());

    Helpers.printPartitionRanges(pRanges50, "TestTable50");

    // fetch w/ partition full coverage
    List<PartitionRange> pRangesFull = fetcher.getPartitionRangesWithCoverage(String.valueOf(0),
        String.valueOf(Integer.MAX_VALUE),
        partitionCount,
        BigDecimal.ONE);

    assertEquals(pRangesFull.size(), (long)partitionCount);
    assertEquals("0", pRangesFull.get(0).getStartRange());
    assertEquals(String.valueOf(Integer.MAX_VALUE), pRangesFull.get(partitionCount-1).getEndRange());

    String range1EndStr = pRangesFull.get(1).getEndRange();
    String range2StartStr = pRangesFull.get(2).getStartRange();
    Integer range1End = Integer.parseInt(range1EndStr);
    Integer range2Start = Integer.parseInt(range2StartStr);
    assertTrue(range1End + 1 == range2Start);

    Helpers.printPartitionRanges(pRangesFull, "TestTableFull");

    assertEquals(pRanges50.get(partitionCount-1).getStartRange(),
        pRangesFull.get(partitionCount-1).getStartRange());
  }

  @Test
  public void uuidPartitionRangeTest() {
    String fieldType = TableSpec.UUID_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    Integer partitionCount = 100;

    // fetch w/ 50% coverage
    List<PartitionRange> pRanges50 =
        fetcher.getPartitionRangesWithCoverage("00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
            partitionCount,
        BigDecimal.valueOf(0.5));

    assertEquals(pRanges50.size(), (long)partitionCount);
    assertEquals("00000000-0000-0000-0000-000000000000", pRanges50.get(0).getStartRange());
    assertNotEquals("ffffffff-ffff-ffff-ffff-ffffffffffff",
        pRanges50.get(partitionCount-1).getEndRange());

    BigInteger uuidMax =
        UUIDHelpers.uuidToBigInt(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));
    BigInteger stepSize = uuidMax.divide(BigInteger.valueOf(partitionCount));
    String endRangeStr = pRanges50.get(partitionCount-1).getEndRange();
    String lastRangeStartCalc = UUIDHelpers.bigIntToUUID(uuidMax.subtract(stepSize)).toString();
    assertTrue(endRangeStr.compareTo(lastRangeStartCalc) > 0);

    assertNotEquals(pRanges50.get(1).getEndRange(), pRanges50.get(2).getStartRange());

    Helpers.printPartitionRanges(pRanges50, "TestTable50");

    // now fetch with full coverage
    List<PartitionRange> pRangesFull =
        fetcher.getPartitionRangesWithCoverage("00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            partitionCount,
            BigDecimal.valueOf(1));

    assertEquals(pRangesFull.size(), (long)partitionCount);
    assertEquals("00000000-0000-0000-0000-000000000000", pRangesFull.get(0).getStartRange());
    assertEquals("ffffffff-ffff-ffff-ffff-ffffffffffff",
        pRangesFull.get(partitionCount-1).getEndRange());

    String range1EndStr = pRangesFull.get(1).getEndRange();
    String range2StartStr = pRangesFull.get(2).getStartRange();
    BigInteger range1End = UUIDHelpers.uuidToBigInt(UUID.fromString(range1EndStr));
    BigInteger range2Start = UUIDHelpers.uuidToBigInt(UUID.fromString(range2StartStr));
    assertTrue(range1End.add(BigInteger.ONE).compareTo(range2Start) == 0);

    assertEquals(pRanges50.get(partitionCount-1).getStartRange(),
        pRangesFull.get(partitionCount-1).getStartRange());

    Helpers.printPartitionRanges(pRangesFull, "TestTableFull");
  }
} // class