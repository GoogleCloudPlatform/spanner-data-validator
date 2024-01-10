package com.google.migration.dto;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.migration.Helpers;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@DefaultCoder(AvroCoder.class)
public class HashResult {
  private static final Logger LOG = LoggerFactory.getLogger(HashResult.class);
  public Boolean isSource;
  public String origValue;
  public String sha256;
  public String key;
  public String range = "";
  public String rangeFieldType = TableSpec.UUID_FIELD_TYPE;

  public HashResult() {
  }

  public HashResult(String keyIn,
      Boolean isSourceIn,
      String origValueIn,
      String sha256In) {
    key = keyIn;
    isSource = isSourceIn;
    origValue = origValueIn;
    sha256 = sha256In;
  }

  public static HashResult fromSpannerStruct(Struct spannerStruct,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision) {
    HashResult retVal = new HashResult();

    int nCols = spannerStruct.getColumnCount();
    StringBuilder sbConcatCols = new StringBuilder();
    for(int i = 0; i < nCols; i++) {
      Type colType = spannerStruct.getColumnType(i);

      switch (colType.toString()) {
        case "STRING":
          sbConcatCols.append(spannerStruct.getString(i));
          break;
        case "INT64":
          sbConcatCols.append(spannerStruct.getLong(i));
          break;
        case "TIMESTAMP":
          Long rawTimestamp = spannerStruct.getTimestamp(i).toSqlTimestamp().getTime();
          if(adjustTimestampPrecision) rawTimestamp = rawTimestamp/1000;
          sbConcatCols.append(rawTimestamp);
          break;
        case "BOOL":
        case "BOOLEAN":
          sbConcatCols.append(spannerStruct.getBoolean(i));
          break;
        default:
          throw new RuntimeException(String.format("Unsupported type: %s", colType));
      } // switch
    } // for

    retVal.key = spannerStruct.getString(keyIndex);
    retVal.rangeFieldType = rangeFieldType;
    retVal.origValue = sbConcatCols.toString();
    retVal.sha256 = Helpers.sha256(retVal.origValue);
    retVal.isSource = true;

    return retVal;
  }

  public static HashResult fromJDBCResultSet(ResultSet resultSet,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision)
      throws SQLException, NoSuchAlgorithmException {
    HashResult retVal = new HashResult();

    ResultSetMetaData rsMetaData = resultSet.getMetaData();
    int colCount = rsMetaData.getColumnCount();
    StringBuilder sbConcatCols = new StringBuilder();
    for(int i = 0; i < colCount; i++) {
      int colOrdinal = i+1;
      int type = rsMetaData.getColumnType(colOrdinal);

      // https://docs.oracle.com/javase/8/docs/api/constant-values.html
      // look for (java.sql.Types)
      switch (type) {
        case Types.VARCHAR:
        case Types.OTHER:
          sbConcatCols.append(resultSet.getString(colOrdinal));
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          sbConcatCols.append(resultSet.getBoolean(colOrdinal));
          break;
        case Types.INTEGER:
          sbConcatCols.append(resultSet.getInt(colOrdinal));
          break;
        case Types.BIGINT:
          sbConcatCols.append(resultSet.getLong(colOrdinal));
        case Types.TIMESTAMP:
        case Types.TIME_WITH_TIMEZONE:
          Long rawTimestamp = resultSet.getTimestamp(colOrdinal).getTime();
          if(adjustTimestampPrecision) rawTimestamp = rawTimestamp/1000;
          sbConcatCols.append(rawTimestamp);
          break;
        default:
          LOG.error(String.format("Unsupported type: %d", type));
          throw new RuntimeException(String.format("Unsupported type: %d", type));
      } // switch
    } // for

    retVal.key = resultSet.getString(keyIndex+1);
    retVal.rangeFieldType = rangeFieldType;
    retVal.origValue = sbConcatCols.toString();
    retVal.sha256 = Helpers.sha256(retVal.origValue);
    retVal.isSource = true;

    return retVal;
  }
} // HashResult