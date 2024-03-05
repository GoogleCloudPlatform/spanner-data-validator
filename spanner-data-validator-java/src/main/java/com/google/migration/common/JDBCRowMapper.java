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