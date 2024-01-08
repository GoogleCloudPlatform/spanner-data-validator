package com.google;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.migration.Helpers;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.UUID;
import org.junit.Test;

public class SpannerReadTest {
  @Test
  public void simpleReadTest() throws Exception {
    SpannerOptions options = SpannerOptions.newBuilder().build();
    Spanner spanner = options.getService();
    try {
      DatabaseId db =
          DatabaseId.of("kt-shared-project", "tempus-test1", "tempus_db1");
      DatabaseClient dbClient = spanner.getDatabaseClient(db);

      try (ResultSet resultSet =
          dbClient
              .singleUse()
              .read(
                  "data_product_records",
                  KeySet.all(), // Read all rows in a table.
                  Arrays.asList("id", "type", "bucket"))) {
        int count = 0;
        while (resultSet.next()) {
          System.out.printf(
              "%s %s %s\n", resultSet.getString(0), resultSet.getString(1),
              resultSet.getString(2));
          count++;
          if(count > 10) {
            break;
          }
        }
      }
    } finally {
      spanner.close();
    } // try/finally
  }
} // class SpannerReadTest
