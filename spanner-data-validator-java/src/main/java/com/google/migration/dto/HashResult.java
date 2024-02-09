package com.google.migration.dto;

import com.google.cloud.spanner.Struct;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Type;
import com.google.migration.Helpers;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONException;
import org.json.JSONObject;
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
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getString(i));
          break;
        case "JSON":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getJson(i));
          break;
        case "JSON<PG_JSONB>":
          if(!spannerStruct.isNull(i)) {
            sbConcatCols.append(getNormalizedJsonString(spannerStruct.getPgJsonb(i)));
          }
          break;
        case "BYTES":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : Base64.encodeBase64String(spannerStruct.getBytes(i).toByteArray()));
          break;
        case "INT64":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getLong(i));
          break;
        case "TIMESTAMP":
          // TODO: This uses millisecond precision; consider using microsecond precision
          if(!spannerStruct.isNull(i)) {
            Long rawTimestamp = spannerStruct.getTimestamp(i).toSqlTimestamp().getTime();
            if (adjustTimestampPrecision)
              rawTimestamp = rawTimestamp / 1000;
            sbConcatCols.append(rawTimestamp);
          }
          break;
        case "BOOL":
        case "BOOLEAN":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getBoolean(i));
          break;
        default:
          throw new RuntimeException(String.format("Unsupported type: %s", colType));
      } // switch
    } // for

    switch(rangeFieldType) {
      case TableSpec.UUID_FIELD_TYPE:
        retVal.key = spannerStruct.getString(keyIndex);
        break;
      case TableSpec.TIMESTAMP_FIELD_TYPE:
        retVal.key = spannerStruct.getTimestamp(keyIndex).toSqlTimestamp().toString();
        break;
      case TableSpec.INT_FIELD_TYPE:
      case TableSpec.LONG_FIELD_TYPE:
        retVal.key = String.valueOf(spannerStruct.getLong(keyIndex));
        break;
      default:
        throw new RuntimeException(String.format("Unexpected range field type %s", rangeFieldType));
    }

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
      throws SQLException {
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
        case Types.CHAR:
        case Types.VARCHAR:
          String val = resultSet.getString(colOrdinal);
          if(val != null) {
            sbConcatCols.append(val);
          }
          break;
          // TODO: we're assuming OTHER is jsonb (FIX)
        case Types.OTHER:
          String otherVal = resultSet.getString(colOrdinal);
          if(otherVal != null) {
            sbConcatCols.append(getNormalizedJsonString(otherVal));
          }
          break;
        case Types.LONGVARBINARY:
          byte[] bytes = resultSet.getBytes(colOrdinal);
          if(bytes != null) {
            sbConcatCols.append(Base64.encodeBase64String(bytes));
          }
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          Boolean boolVal = resultSet.getBoolean(colOrdinal);
          if(boolVal != null) {
            sbConcatCols.append(boolVal);
          }
          break;
        case Types.INTEGER:
          Integer intVal = resultSet.getInt(colOrdinal);
          if(intVal != null) {
            sbConcatCols.append(intVal);
          }
          break;
        case Types.BIGINT:
          Long longVal = resultSet.getLong(colOrdinal);
          if(longVal != null) {
            sbConcatCols.append(longVal);
          }
          break;
        case Types.DECIMAL:
          BigDecimal decimalVal = resultSet.getBigDecimal(colOrdinal);
          if(decimalVal != null) {
            sbConcatCols.append(decimalVal);
          }
          break;
        case Types.TIMESTAMP:
        case Types.TIME_WITH_TIMEZONE:
          // TODO: This uses millisecond precision; consider using microsecond precision
          java.sql.Timestamp timestampVal = resultSet.getTimestamp(colOrdinal);
          if(timestampVal != null) {
            Long rawTimestamp = timestampVal.getTime();
            if (adjustTimestampPrecision)
              rawTimestamp = rawTimestamp / 1000;
            sbConcatCols.append(rawTimestamp);
          }
          break;
        default:
          LOG.error(String.format("Unsupported type: %d", type));
          throw new RuntimeException(String.format("Unsupported type: %d", type));
      } // switch
    } // for

    switch(rangeFieldType) {
      case TableSpec.UUID_FIELD_TYPE:
        retVal.key = resultSet.getString(keyIndex+1);
        break;
      case TableSpec.TIMESTAMP_FIELD_TYPE:
        retVal.key = resultSet.getTimestamp(keyIndex+1).toString();
        break;
      case TableSpec.INT_FIELD_TYPE:
        retVal.key = String.valueOf(resultSet.getInt(keyIndex+1));
        break;
      case TableSpec.LONG_FIELD_TYPE:
        retVal.key = String.valueOf(resultSet.getLong(keyIndex+1));
        break;
      default:
        throw new RuntimeException(String.format("Unexpected range field type %s", rangeFieldType));
    }

    retVal.rangeFieldType = rangeFieldType;
    retVal.origValue = sbConcatCols.toString();
    retVal.sha256 = Helpers.sha256(retVal.origValue);
    retVal.isSource = true;

    return retVal;
  }

  private static String getNormalizedJsonString(String rawJsonInput) {
    ObjectMapper om = new ObjectMapper();
    try {
      // https://stackoverflow.com/questions/30325042/how-to-compare-two-json-strings-when-the-order-of-entries-keep-changing
      TreeMap<String, Object> m1 = (TreeMap<String, Object>) (om.readValue(rawJsonInput, TreeMap.class));

      return om.writeValueAsString(m1);
    }catch (Exception ex) {
    }

    return rawJsonInput;
  }
} // HashResult