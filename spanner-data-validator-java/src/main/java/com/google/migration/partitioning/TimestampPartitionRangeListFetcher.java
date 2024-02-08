package com.google.migration.partitioning;

import com.google.migration.dto.PartitionRange;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampPartitionRangeListFetcher extends LongPartitionRangeListFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(TimestampPartitionRangeListFetcher.class);

  @Override
  public List<PartitionRange> getPartitionRanges(Integer partitionCount) {
    List<PartitionRange> pRangesLong = super.getPartitionRangesWithCoverage(
        String.valueOf(Timestamp.valueOf("1971-01-01 00:00:00").getTime()),
        String.valueOf(Timestamp.valueOf("2035-01-01 00:00:00").getTime()),
        partitionCount,
        BigDecimal.ONE);

    return convertLongToTimestamp(pRangesLong);
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(Integer partitionCount,
      BigDecimal coveragePercent) {
    List<PartitionRange> pRangesLong = super.getPartitionRangesWithCoverage(
        String.valueOf(Timestamp.valueOf("1971-01-01 00:00:00").getTime()),
        String.valueOf(Timestamp.valueOf("2035-01-01 00:00:00").getTime()),
        partitionCount,
        coveragePercent);

    return convertLongToTimestamp(pRangesLong);
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithCoverage(String startStr,
      String endStr,
      Integer partitionCount,
      BigDecimal coveragePercent) {
    Timestamp startRange = getTimestampFromString(startStr);
    Timestamp endRange = getTimestampFromString(endStr);

    List<PartitionRange> pRangesLong = super.getPartitionRangesWithCoverage(
        String.valueOf(startRange.getTime()),
        String.valueOf(endRange.getTime()),
        partitionCount,
        coveragePercent);

    return  convertLongToTimestamp(pRangesLong);
  }

  @Override
  public List<PartitionRange> getPartitionRangesWithPartitionFilter(String startStr,
      String endStr,
      Integer partitionCount,
      Integer partitionFilterRatio) {
    List<PartitionRange> pRangesLong = super.getPartitionRangesWithPartitionFilter(
        String.valueOf(Timestamp.valueOf(startStr).getTime()),
        String.valueOf(Timestamp.valueOf(endStr).getTime()),
        partitionCount,
        partitionFilterRatio);

    return  convertLongToTimestamp(pRangesLong);
  }

  private List<PartitionRange> convertLongToTimestamp(List<PartitionRange> partitionRangesIn) {
    ArrayList<PartitionRange> retVal = new ArrayList<>();

    for(PartitionRange pRangeIn: partitionRangesIn) {
      long startRangeMicroSeconds = Long.parseLong(pRangeIn.getStartRange());
      long endRangeMicroSeconds = Long.parseLong(pRangeIn.getEndRange());
      Timestamp startRange = Timestamp.from(Instant.ofEpochMilli(startRangeMicroSeconds));
      Timestamp endRange = Timestamp.from(Instant.ofEpochMilli(endRangeMicroSeconds));

      // https://docs.oracle.com/javase/8/docs/api/java/sql/Timestamp.html#toString--
      retVal.add(new PartitionRange(startRange.toString(), endRange.toString()));
    }

    return retVal;
  }

  private Timestamp getTimestampFromString(String valueIn) {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    try {
      LocalDateTime dateTime = LocalDateTime.parse(valueIn, dateTimeFormatter);
      return Timestamp.valueOf(dateTime);
    } catch(DateTimeParseException parseException) {
      LocalDate date = LocalDate.parse(valueIn);
      return Timestamp.valueOf(date.atStartOfDay());
    } // try/catch
  }
} // class TimestampPartitionRangeListFetcher