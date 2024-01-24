package com.google;

import static org.junit.Assert.assertEquals;

import com.google.migration.dto.PartitionRange;
import com.google.migration.dto.TableSpec;
import com.google.migration.partitioning.PartitionRangeListFetcher;
import com.google.migration.partitioning.PartitionRangeListFetcherFactory;
import java.util.List;
import org.junit.Test;

public class PartitionRangeTest {

  @Test
  public void longPartitionRangeTest() {
    String fieldType = TableSpec.LONG_FIELD_TYPE;

    PartitionRangeListFetcher fetcher =
        PartitionRangeListFetcherFactory.getFetcher(fieldType);
    List<PartitionRange> pRanges = fetcher.getPartitionRangesWithPartitionFilter(String.valueOf(0L),
        String.valueOf(Long.MAX_VALUE),
        1000000000,
        100000000);

    assertEquals(pRanges.size(), 12);

    System.out.println(String.format("test: %s", pRanges.get(1).getEndRange()));
  }
} // class