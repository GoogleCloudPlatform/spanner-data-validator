package com.google.migration;

import com.google.migration.dto.SourceRecord;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class SourceRecordMapper implements RowMapper<SourceRecord> {

  @Override
  public SourceRecord mapRow(@UnknownKeyFor @NonNull @Initialized ResultSet resultSet)
      throws @UnknownKeyFor@NonNull@Initialized Exception {
    SourceRecord sourceRecord = new SourceRecord();
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    int colCount = resultSetMetaData.getColumnCount();
    for (int colOrdinal = 1; colOrdinal <= colCount; colOrdinal++) {
      int type = resultSetMetaData.getColumnType(colOrdinal);
      switch (type) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
          sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal), resultSetMetaData.getColumnTypeName(colOrdinal), resultSet.getString(colOrdinal));
          break;
        case Types.INTEGER:
          sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal), resultSetMetaData.getColumnTypeName(colOrdinal), resultSet.getInt(colOrdinal));
          break;
        case Types.DOUBLE:
          sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal), resultSetMetaData.getColumnTypeName(colOrdinal), resultSet.getDouble(colOrdinal));
          break;
        case Types.REAL:
          sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal), resultSetMetaData.getColumnTypeName(colOrdinal), resultSet.getFloat(colOrdinal));
          break;
        case Types.TINYINT:
          sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal), resultSetMetaData.getColumnTypeName(colOrdinal), resultSet.getShort(colOrdinal));
          break;
        case Types.BIGINT:
          sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal), resultSetMetaData.getColumnTypeName(colOrdinal), resultSet.getLong(colOrdinal));
          break;
        case Types.DECIMAL:
          sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal), resultSetMetaData.getColumnTypeName(colOrdinal), resultSet.getBigDecimal(colOrdinal));
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal), resultSetMetaData.getColumnTypeName(colOrdinal), resultSet.getBoolean(colOrdinal));
          break;
      }
    }
    return sourceRecord;
  }
}
