package com.google.migration;

import com.google.migration.dto.SourceRecord;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceRecordMapper implements RowMapper<SourceRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SourceRecordMapper.class);

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
          String stringVal = resultSet.getString(colOrdinal);
          if (stringVal != null && !resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), stringVal);
          }
          break;
        case Types.INTEGER:
          Integer intVal = resultSet.getInt(colOrdinal);
          if (!resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), intVal);
          }
          break;
        case Types.DOUBLE:
          Double doubleVal = resultSet.getDouble(colOrdinal);
          if (!resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), doubleVal);
          }
          break;
        case Types.REAL:
          Float floatVal = resultSet.getFloat(colOrdinal);
          if (!resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), floatVal);
          }
          break;
        case Types.TINYINT:
          Short shortVal = resultSet.getShort(colOrdinal);
          if (!resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), shortVal);
          }
          break;
        case Types.BIGINT:
          Long longVal = resultSet.getLong(colOrdinal);
          if (!resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), longVal);
          }
          break;
        case Types.DECIMAL:
          BigDecimal decimalVal = resultSet.getBigDecimal(colOrdinal);
          if (!resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), decimalVal);
          }
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          Boolean boolVal = resultSet.getBoolean(colOrdinal);
          if (!resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), boolVal);
          }
          break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
          byte[] bytesVal = resultSet.getBytes(colOrdinal);
          if (bytesVal != null && !resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), bytesVal);
          }
          break;
        case Types.DATE:
          Date dateVal = resultSet.getDate(colOrdinal);
          if (dateVal != null && !resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), dateVal);
          }
          break;
        case Types.TIMESTAMP:
        case Types.TIME_WITH_TIMEZONE:
          Timestamp timestampVal = resultSet.getTimestamp(colOrdinal);
          if (timestampVal != null && !resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), timestampVal);
          }
          break;
        case Types.OTHER:
          String otherVal = resultSet.getString(colOrdinal);
          if (otherVal != null && !resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), otherVal);
          }
        case Types.ARRAY:
          Array arrayVal = resultSet.getArray(colOrdinal);
          if (arrayVal != null && !resultSet.wasNull()) {
            sourceRecord.addField(resultSetMetaData.getColumnName(colOrdinal),
                resultSetMetaData.getColumnTypeName(colOrdinal), arrayVal);
          }
        default:
          LOG.error("Unsupported type: {}", type);
          throw new RuntimeException(String.format("Unsupported type: %d", type));
      }
    }
    return sourceRecord;
  }
}
