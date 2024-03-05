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

import com.google.migration.Helpers;
import com.google.migration.dofns.MapWithRangeFn;
import com.google.migration.dofns.MapWithRangeFn.MapWithRangeType;
import com.google.migration.dto.HashResult;
import com.google.migration.dto.PartitionRange;
import com.google.migration.dto.TableSpec;
import com.google.migration.partitioning.PartitionRangeListFetcher;
import com.google.migration.partitioning.PartitionRangeListFetcherFactory;
import java.math.BigInteger;
import java.util.List;
import java.util.UUID;
import org.junit.Test;

public class MapWithRangeFnTest {
  @Test
  public void mapWithRangeForIntTest()  {
    String fieldType = TableSpec.INT_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);
    List<PartitionRange> pRanges = fetcher.getPartitionRanges(100);
    assertEquals(pRanges.size(), 100);

    HashResult hr = new HashResult("0", true, "orig", "hash");
    MapWithRangeFn mapFn = new MapWithRangeFn(null,
        MapWithRangeType.RANGE_PLUS_HASH,
        fieldType);

    PartitionRange pRange = mapFn.getPartitionRangeForRecord(hr, pRanges);
    assertEquals(pRange.getStartRange(), "0");

    hr.key = "1";
    pRange = mapFn.getPartitionRangeForRecord(hr, pRanges);
    assertEquals(pRange.getStartRange(), "0");

    hr.key = String.valueOf(Integer.MAX_VALUE - 1);
    pRange = mapFn.getPartitionRangeForRecord(hr, pRanges);
    assertNotEquals(pRange.getStartRange(), "1");
  }

  @Test
  public void mapWithRangeForLongTest()  {
    String fieldType = TableSpec.LONG_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);
    List<PartitionRange> pRanges = fetcher.getPartitionRanges(100);
    assertEquals(pRanges.size(), 100);

    for(PartitionRange pRange: pRanges) {
      System.out.println(String.format("Start: %s, end: %s", pRange.getStartRange(), pRange.getEndRange()));
    }

    HashResult hr = new HashResult("0", true, "orig", "hash");
    MapWithRangeFn mapFn = new MapWithRangeFn(null,
        MapWithRangeType.RANGE_PLUS_HASH,
        fieldType);

    PartitionRange pRange = mapFn.getPartitionRangeForRecord(hr, pRanges);
    assertEquals(pRange.getStartRange(), "0");

    hr.key = "1";
    pRange = mapFn.getPartitionRangeForRecord(hr, pRanges);
    assertEquals(pRange.getStartRange(), "0");

    hr.key = String.valueOf(Long.MAX_VALUE - 1);
    pRange = mapFn.getPartitionRangeForRecord(hr, pRanges);
    assertNotEquals(pRange.getStartRange(), "1");
  }

  @Test
  public void mapWithRangeForUUIDTest()  {
    String fieldType = TableSpec.UUID_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);
    List<PartitionRange> pRanges = fetcher.getPartitionRanges(100);
    assertEquals(pRanges.size(), 100);

    BigInteger val = BigInteger.ZERO;
    UUID zeroUUID = Helpers.bigIntToUUID(val);
    HashResult hr = new HashResult(zeroUUID.toString(), true, "orig", "hash");
    MapWithRangeFn mapFn = new MapWithRangeFn(null,
        MapWithRangeType.RANGE_PLUS_HASH,
        fieldType);

    PartitionRange pRange = mapFn.getPartitionRangeForRecord(hr, pRanges);
    assertEquals(pRange.getStartRange(), zeroUUID.toString());

    val = BigInteger.ONE;
    UUID oneUUID = Helpers.bigIntToUUID(val);
    hr.key = oneUUID.toString();
    pRange = mapFn.getPartitionRangeForRecord(hr, pRanges);
    assertEquals(pRange.getStartRange(), zeroUUID.toString());
  }
} // class MapWithRangeFnTest