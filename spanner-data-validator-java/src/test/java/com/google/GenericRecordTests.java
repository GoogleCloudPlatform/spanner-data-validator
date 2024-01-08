package com.google;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.migration.Helpers;
import java.io.InputStream;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;
import org.junit.Test;
import org.mockito.Mockito;

public class GenericRecordTests {
  @Test
  public void fieldListTest() throws Exception {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    InputStream is = classloader.getResourceAsStream("avro/data_product_metadata.avsc");
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(is);
  }

  @Test
  public void postgresResultSetTest() throws Exception {
    ResultSet mockedResultSet = mockResultSetAndMetadata();

    while(mockedResultSet.next()) {
      ResultSetMetaData rsMetaData = mockedResultSet.getMetaData();
      int colCount = rsMetaData.getColumnCount();
      for(int i = 0; i < colCount; i++) {
        int colOrdinal = i+1;
        String colName = rsMetaData.getColumnName(colOrdinal);
        int type = rsMetaData.getColumnType(colOrdinal);
        String val = mockedResultSet.getString(colOrdinal);
        System.out.println(String.format("Col name: %s, type: %d, val: %s", colName, type, val));
      } // for
    } // while
  }

  private ResultSet mockResultSetAndMetadata() throws SQLException {
    // prepare the dependant mock
    ResultSetMetaData rsMetaMock = Mockito.mock(ResultSetMetaData.class);
    when(rsMetaMock.getColumnName(eq(1))).thenReturn("col1");
    when(rsMetaMock.getColumnType(eq(1))).thenReturn(Types.VARCHAR);
    when(rsMetaMock.getColumnCount()).thenReturn(1);

    // prepare main mock for result set and define when
    // main mock to return dependant mock
    ResultSet rs = Mockito.mock(ResultSet.class);
    when(rs.getMetaData()).thenReturn(rsMetaMock);

    when(rs.getString("col1")).thenReturn("col1-val");
    when(rs.getString(1)).thenReturn("col1-val");
    when(rs.next()).thenReturn(true).thenReturn(false);

    return rs;
  }
} // class GenericRecordTests