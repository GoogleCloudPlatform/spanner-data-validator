package com.google;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.migration.dto.PartitionRange;
import com.google.migration.dto.TableSpec;
import com.google.migration.partitioning.PartitionRangeListFetcher;
import com.google.migration.partitioning.PartitionRangeListFetcherFactory;
import com.google.migration.partitioning.UUIDHelpers;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class PartitionRangeTest {

  @Test
  public void longPartitionRangeTest() {
    String fieldType = TableSpec.LONG_FIELD_TYPE;

    // fetch w/ partition filter ratio
    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);
    List<PartitionRange> pRanges = fetcher.getPartitionRangesWithPartitionFilter(String.valueOf(0L),
        String.valueOf(Long.MAX_VALUE),
        1000000000,
        100000000);

    assertEquals(pRanges.size(), 12);

    // fetch w/ partition full coverage
    pRanges = fetcher.getPartitionRangesWithCoverage(String.valueOf(0L),
        String.valueOf(Long.MAX_VALUE),
        100,
        BigDecimal.ONE);

    assertEquals(pRanges.size(), 101);
    assertEquals("0", pRanges.get(0).getStartRange());
    assertEquals("0", pRanges.get(0).getEndRange());
    assertEquals(String.valueOf(Long.MAX_VALUE), pRanges.get(100).getEndRange());

    // fetch w/ partition 50% coverage
    pRanges = fetcher.getPartitionRangesWithCoverage(String.valueOf(0L),
        String.valueOf(Long.MAX_VALUE),
        100,
        BigDecimal.valueOf(0.5));

    assertEquals(pRanges.size(), 101);
    assertEquals("0", pRanges.get(0).getStartRange());
    assertEquals("0", pRanges.get(0).getEndRange());
    assertNotEquals(String.valueOf(Long.MAX_VALUE), pRanges.get(100).getEndRange());
  }

  @Test
  public void int32PartitionRangeTest() {
    String fieldType = TableSpec.INT_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    // fetch w/ partition full coverage
    List<PartitionRange> pRanges = fetcher.getPartitionRangesWithCoverage(String.valueOf(0),
        String.valueOf(Integer.MAX_VALUE),
        100,
        BigDecimal.ONE);

    assertEquals(pRanges.size(), 101);
    assertEquals("0", pRanges.get(0).getStartRange());
    assertEquals("0", pRanges.get(0).getEndRange());
    assertEquals(String.valueOf(Integer.MAX_VALUE), pRanges.get(100).getEndRange());

    // fetch w/ partition 50% coverage
    pRanges = fetcher.getPartitionRangesWithCoverage(String.valueOf(0),
        String.valueOf(Integer.MAX_VALUE),
        100,
        BigDecimal.valueOf(0.5));

    assertEquals(pRanges.size(), 101);
    assertEquals("0", pRanges.get(0).getStartRange());
    assertEquals("0", pRanges.get(0).getEndRange());
    assertNotEquals(String.valueOf(Integer.MAX_VALUE), pRanges.get(100).getEndRange());
  }

  @Test
  public void uuidPartitionRangeTest() {
    String fieldType = TableSpec.UUID_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    // fetch w/ 50% coverage
    List<PartitionRange> pRanges =
        fetcher.getPartitionRangesWithCoverage("00000000-0000-0000-0000-000000000000",
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        100,
        BigDecimal.valueOf(0.5));

    assertEquals(pRanges.size(), 101);
    assertEquals("00000000-0000-0000-0000-000000000000", pRanges.get(0).getStartRange());
    assertEquals("00000000-0000-0000-0000-000000000000", pRanges.get(0).getEndRange());
    assertNotEquals("ffffffff-ffff-ffff-ffff-ffffffffffff", pRanges.get(100).getEndRange());

    BigInteger uuidMax =
        UUIDHelpers.uuidToBigInt(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));
    BigInteger stepSize = uuidMax.divide(BigInteger.valueOf(100));
    String endRangeStr = pRanges.get(100).getEndRange();
    String lastRangeStartCalc = UUIDHelpers.bigIntToUUID(uuidMax.subtract(stepSize)).toString();
    assertTrue(endRangeStr.compareTo(lastRangeStartCalc) > 0);

    assertNotEquals(pRanges.get(1).getEndRange(), pRanges.get(2).getStartRange());

    // now fetch with full coverage
    pRanges =
        fetcher.getPartitionRangesWithCoverage("00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            100,
            BigDecimal.valueOf(1));

    assertEquals(pRanges.size(), 101);
    assertEquals("00000000-0000-0000-0000-000000000000", pRanges.get(0).getStartRange());
    assertEquals("00000000-0000-0000-0000-000000000000", pRanges.get(0).getEndRange());
    assertEquals("ffffffff-ffff-ffff-ffff-ffffffffffff", pRanges.get(100).getEndRange());

    assertEquals(pRanges.get(1).getEndRange(), pRanges.get(2).getStartRange());
  }
} // class