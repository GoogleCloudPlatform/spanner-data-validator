/*
 Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

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
  public void longSinglePartitionRangeTest() {
    String fieldType = TableSpec.LONG_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    Integer partitionCount = 1;

    // fetch w/ 50% coverage
    List<PartitionRange> pRanges50 =
        fetcher.getPartitionRangesWithCoverage("0",
            String.valueOf(Long.MAX_VALUE),
            partitionCount,
            BigDecimal.valueOf(0.5));

    assertEquals(pRanges50.size(), (long)partitionCount);
    assertEquals("0", pRanges50.get(0).getStartRange());
    assertNotEquals(String.valueOf(Long.MAX_VALUE), pRanges50.get(0).getEndRange());

    Long endRangeVal = Long.parseLong(pRanges50.get(0).getEndRange());

    // validate that there's no overflow
    assertTrue(endRangeVal > 0);

    // validate that end range < Long.MAX_VALUE
    assertTrue(endRangeVal < Long.MAX_VALUE);

    Helpers.printPartitionRanges(pRanges50, "TestTable50");

    // now fetch with full coverage
    List<PartitionRange> pRangesFull =
        fetcher.getPartitionRangesWithCoverage("0",
            String.valueOf(Long.MAX_VALUE),
            partitionCount,
            BigDecimal.ONE);

    assertEquals(pRangesFull.size(), (long)partitionCount);
    assertEquals("0", pRangesFull.get(0).getStartRange());
    assertEquals(String.valueOf(Long.MAX_VALUE), pRangesFull.get(0).getEndRange());

    Helpers.printPartitionRanges(pRangesFull, "TestTableFull");
  }

  @Test
  public void timestampPartitionRangeTest() {
    String fieldType = TableSpec.TIMESTAMP_FIELD_TYPE;

    // fetch w/ partition filter ratio
    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    Integer partitionCount = 100;

    // fetch w/ partition 50% coverage
    List<PartitionRange> pRanges50 = fetcher.getPartitionRangesWithCoverage(
        "1971-01-01 00:00:00",
        "2023-10-04T19:29:43.067Z",
        partitionCount,
        BigDecimal.valueOf(0.5));

    assertEquals(pRanges50.size(), (long)partitionCount);
    assertEquals("1971-01-01 00:00:00.0", pRanges50.get(0).getStartRange());
    assertNotEquals(String.valueOf(Long.MAX_VALUE), pRanges50.get(partitionCount-1).getEndRange());

    Helpers.printPartitionRanges(pRanges50, "TestTable50");

    // fetch w/ partition full coverage
    List<PartitionRange> pRangesFull = fetcher.getPartitionRangesWithCoverage(
        "1971-01-01",
        "2023-10-04T19:29:43.067Z",
        partitionCount,
        BigDecimal.ONE);

    assertEquals(pRangesFull.size(), (long)partitionCount);
    assertEquals("1971-01-01 00:00:00.0", pRangesFull.get(0).getStartRange());

    Helpers.printPartitionRanges(pRangesFull, "TestTableFull");

    assertEquals(pRanges50.get(partitionCount-1).getStartRange(),
        pRangesFull.get(partitionCount-1).getStartRange());
  }

  @Test
  public void timestampSinglePartitionRangeTest() {
    String fieldType = TableSpec.TIMESTAMP_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    Integer partitionCount = 1;

    // fetch w/ 50% coverage
    List<PartitionRange> pRanges50 = fetcher.getPartitionRangesWithCoverage(
        "1971-01-01 00:00:00",
        "2025-01-01 00:00:00",
        partitionCount,
        BigDecimal.valueOf(0.5));

    assertEquals(pRanges50.size(), (long)partitionCount);
    assertEquals("1971-01-01 00:00:00.0", pRanges50.get(0).getStartRange());

    Helpers.printPartitionRanges(pRanges50, "TestTable50");

    // now fetch with full coverage
    List<PartitionRange> pRangesFull = fetcher.getPartitionRangesWithCoverage(
        "1971-01-01",
        "2025-01-01",
        partitionCount,
        BigDecimal.ONE);

    assertEquals(pRangesFull.size(), (long)partitionCount);
    assertEquals("1971-01-01 00:00:00.0", pRanges50.get(0).getStartRange());

    Helpers.printPartitionRanges(pRangesFull, "TestTableFull");
  }

  @Test
  public void stringPartitionRangeTest() {
    String fieldType = TableSpec.STRING_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    Integer partitionCount = 1;

    List<PartitionRange> pRanges = fetcher.getPartitionRangesWithCoverage(
        "a",
        "b",
        partitionCount,
        BigDecimal.ONE);

    assertEquals(pRanges.size(), (long)partitionCount);

    Helpers.printPartitionRanges(pRanges, "TestStringTable");
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
  public void int32SinglePartitionRangeTest() {
    String fieldType = TableSpec.INT_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    Integer partitionCount = 1;

    // fetch w/ 50% coverage
    List<PartitionRange> pRanges50 =
        fetcher.getPartitionRangesWithCoverage("0",
            String.valueOf(Integer.MAX_VALUE),
            partitionCount,
            BigDecimal.valueOf(0.5));

    assertEquals(pRanges50.size(), (long)partitionCount);
    assertEquals("0", pRanges50.get(0).getStartRange());
    assertNotEquals(String.valueOf(Integer.MAX_VALUE), pRanges50.get(0).getEndRange());

    Integer endRangeVal = Integer.parseInt(pRanges50.get(0).getEndRange());

    // validate that there's no overflow
    assertTrue(endRangeVal > 0);

    // validate that end range < Integer.MAX_VALUE
    assertTrue(endRangeVal < Integer.MAX_VALUE);

    Helpers.printPartitionRanges(pRanges50, "TestTable50");

    // now fetch with full coverage
    List<PartitionRange> pRangesFull =
        fetcher.getPartitionRangesWithCoverage("0",
            String.valueOf(Integer.MAX_VALUE),
            partitionCount,
            BigDecimal.ONE);

    assertEquals(pRangesFull.size(), (long)partitionCount);
    assertEquals("0", pRangesFull.get(0).getStartRange());
    assertEquals(String.valueOf(Integer.MAX_VALUE), pRangesFull.get(0).getEndRange());

    Helpers.printPartitionRanges(pRangesFull, "TestTableFull");
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

  @Test
  public void uuidSinglePartitionRangeTest() {
    String fieldType = TableSpec.UUID_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);

    Integer partitionCount = 1;

    UUID uuidMax = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

    // fetch w/ 50% coverage
    List<PartitionRange> pRanges50 =
        fetcher.getPartitionRangesWithCoverage("00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            partitionCount,
            BigDecimal.valueOf(0.5));

    assertEquals(pRanges50.size(), (long)partitionCount);
    PartitionRange range0 = pRanges50.get(0);
    assertEquals("00000000-0000-0000-0000-000000000000", range0.getStartRange());
    assertNotEquals("ffffffff-ffff-ffff-ffff-ffffffffffff", range0.getEndRange());

    // validate that end range < uuidMax
    assertTrue(UUIDHelpers.uuidToBigInt(UUID.fromString(range0.getEndRange()))
        .compareTo(UUIDHelpers.uuidToBigInt(uuidMax)) < 0);

    Helpers.printPartitionRanges(pRanges50, "TestTable50");

    // now fetch with full coverage
    List<PartitionRange> pRangesFull =
        fetcher.getPartitionRangesWithCoverage("00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            partitionCount,
            BigDecimal.valueOf(1));

    assertEquals(pRangesFull.size(), (long)partitionCount);
    assertEquals("00000000-0000-0000-0000-000000000000", pRangesFull.get(0).getStartRange());
    assertEquals("ffffffff-ffff-ffff-ffff-ffffffffffff", pRangesFull.get(0).getEndRange());

    Helpers.printPartitionRanges(pRangesFull, "TestTableFull");
  }
} // class