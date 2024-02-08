package com.google;

import java.sql.Timestamp;
import java.time.Instant;
import org.junit.Test;

public class TimestampTest {
  @Test
  public void timestampMicrosTest() {
    Timestamp.from(Instant.now());
  }
} // class