package com.google;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.junit.Test;

public class TimestampThresholdTest {
  @Test
  public void basicTimestampParsingTest() {
    //String rawInput = "2018-05-05T11:50:55";
    String rawInput = "2024-01-21T19:52:17.0";
    LocalDateTime dateTime = LocalDateTime.parse(rawInput);
    System.out.println(dateTime);
    Instant instant = dateTime.toInstant(ZoneOffset.ofHours(0));
    System.out.println(instant);
    System.out.println(instant.toEpochMilli());
  }
} // class TimestampThresholdTest