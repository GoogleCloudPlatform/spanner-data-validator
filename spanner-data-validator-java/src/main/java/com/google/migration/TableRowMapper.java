package com.google.migration;

import com.google.api.services.bigquery.model.TableRow;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

public class TableRowMapper implements RowMapper<TableRow> {

  @Override
  public TableRow mapRow(@UnknownKeyFor @NonNull @Initialized ResultSet resultSet)
      throws @UnknownKeyFor@NonNull@Initialized Exception {
    TableRow tableRow = new TableRow();
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    int colCount = resultSetMetaData.getColumnCount();
    for (int colOrdinal = 1; colOrdinal <= colCount; colOrdinal++) {
      int type = resultSetMetaData.getColumnType(colOrdinal);
      switch (type) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
          tableRow.set(resultSetMetaData.getColumnName(colOrdinal), resultSet.getString(colOrdinal));
          break;
        case Types.INTEGER:
          tableRow.set(resultSetMetaData.getColumnName(colOrdinal), resultSet.getInt(colOrdinal));
          break;
        case Types.DOUBLE:
          tableRow.set(resultSetMetaData.getColumnName(colOrdinal), resultSet.getDouble(colOrdinal));
          break;
        case Types.REAL:
          tableRow.set(resultSetMetaData.getColumnName(colOrdinal), resultSet.getFloat(colOrdinal));
          break;
        case Types.TINYINT:
          tableRow.set(resultSetMetaData.getColumnName(colOrdinal), resultSet.getShort(colOrdinal));
          break;
        case Types.BIGINT:
          tableRow.set(resultSetMetaData.getColumnName(colOrdinal), resultSet.getLong(colOrdinal));
          break;
        case Types.DECIMAL:
          tableRow.set(resultSetMetaData.getColumnName(colOrdinal), resultSet.getBigDecimal(colOrdinal));
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          tableRow.set(resultSetMetaData.getColumnName(colOrdinal), resultSet.getBoolean(colOrdinal));
          break;
      }
    }
    return tableRow;
  }
}
