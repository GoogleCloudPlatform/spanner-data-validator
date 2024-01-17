package com.google.migration.common;

import com.google.migration.dto.HashResult;
import java.sql.ResultSet;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class JDBCRowMapper implements RowMapper<HashResult> {

  private Integer keyIndex;
  private String rangeFieldType;
  private Boolean adjustTimestampPrecision;

  public JDBCRowMapper(Integer keyIndexIn,
      String rangeFieldTypeIn,
      Boolean adjustTimestampPrecisionIn) {
    keyIndex = keyIndexIn;
    rangeFieldType = rangeFieldTypeIn;
    adjustTimestampPrecision = adjustTimestampPrecisionIn;
  }

  @Override
  public HashResult mapRow(@UnknownKeyFor @NonNull @Initialized ResultSet resultSet)
      throws @UnknownKeyFor@NonNull@Initialized Exception {
    return HashResult.fromJDBCResultSet(resultSet,
        keyIndex,
        rangeFieldType,
        adjustTimestampPrecision);
  }
} // class JDBCRowMapper