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

package com.google.migration.dto;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.migration.Helpers;
import com.google.migration.common.JSONNormalizer;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.commons.codec.binary.Base64;
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
  public Long timestampThresholdValue = 0L;

  public HashResult() {
  }

  @Override
  public String toString() {
    return "HashResult{" +
        "isSource=" + isSource +
        ", origValue='" + origValue + '\'' +
        ", sha256='" + sha256 + '\'' +
        ", key='" + key + '\'' +
        ", range='" + range + '\'' +
        ", rangeFieldType='" + rangeFieldType + '\'' +
        ", timestampThresholdValue=" + timestampThresholdValue +
        '}';
  }

  public HashResult(String keyIn,
      Boolean isSourceIn,
      String origValueIn,
      String sha256In,
      Long timestampThresholdValueIn) {
    key = keyIn;
    isSource = isSourceIn;
    origValue = origValueIn;
    sha256 = sha256In;
    timestampThresholdValue = timestampThresholdValueIn;
  }

  public static HashResult fromSpannerStruct(Struct spannerStruct,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision,
      Integer timestampThresholdIndex) {
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
          if(!spannerStruct.isNull(i)) {
            sbConcatCols.append(getNormalizedJsonString(spannerStruct.getJson(i)));
          }
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
        case "FLOAT64":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getDouble(i));
          break;
        case "NUMERIC":
          sbConcatCols.append(spannerStruct.isNull(i) ? "" : spannerStruct.getBigDecimal(i));
          break;
        case "TIMESTAMP":
          // TODO: This uses millisecond precision; consider using microsecond precision
          if(!spannerStruct.isNull(i)) {
            Long rawTimestamp = spannerStruct.getTimestamp(i).toSqlTimestamp().getTime();
            if (adjustTimestampPrecision)
              rawTimestamp = rawTimestamp / 1000;
            sbConcatCols.append(rawTimestamp);
            if(timestampThresholdIndex >= 0) {
              if(i == timestampThresholdIndex) {
                retVal.timestampThresholdValue = rawTimestamp;
                if(adjustTimestampPrecision)
                  retVal.timestampThresholdValue = retVal.timestampThresholdValue * 1000;
              }
            }
          }
          break;
        case "DATE":
          if(!spannerStruct.isNull(i)) {
            com.google.cloud.Date date = spannerStruct.getDate(i);
            sbConcatCols.append(String.format("%d%d%d",
                date.getYear(),
                date.getMonth(),
                date.getDayOfMonth()));
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
      case TableSpec.STRING_FIELD_TYPE:
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
    retVal.isSource = false;
    LOG.info("SpannerHash=> Key={}, OrigValue={} \n HashResult={}", retVal.key, retVal.origValue, retVal.sha256);
    return retVal;
  }

  public static HashResult fromJDBCResultSet(ResultSet resultSet,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision,
      Integer timestampThresholdIndex)
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
        case Types.LONGVARCHAR:
          String val = resultSet.getString(colOrdinal);
          if(val != null && !resultSet.wasNull()) {
            sbConcatCols.append(val);
          }
          break;
        case Types.ARRAY:
          Array arrayVal = resultSet.getArray(colOrdinal);
          if(arrayVal != null && !resultSet.wasNull()) {
            String[] vals = (String[])arrayVal.getArray();
            for(int j = 0; j < vals.length; j++) {
              sbConcatCols.append(vals[j]);
            }
          }
          break;
        // TODO: we're assuming OTHER is jsonb (FIX)
        case Types.OTHER:
          String otherVal = resultSet.getString(colOrdinal);
          if(otherVal != null && !resultSet.wasNull()) {
            sbConcatCols.append(getNormalizedJsonString(otherVal));
          }
          break;
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
          byte[] bytes = resultSet.getBytes(colOrdinal);
          if(bytes != null && !resultSet.wasNull()) {
            sbConcatCols.append(Base64.encodeBase64String(bytes));
          }
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          Boolean boolVal = resultSet.getBoolean(colOrdinal);
          if(boolVal != null && !resultSet.wasNull()) {
            // https://stackoverflow.com/questions/39561112/getting-boolean-from-resultset
            sbConcatCols.append(boolVal);
          }
          break;
        case Types.INTEGER:
          Integer intVal = resultSet.getInt(colOrdinal);
          if(intVal != null && !resultSet.wasNull()) {
            sbConcatCols.append(intVal);
          }
          break;
        case Types.DOUBLE:
          Double doubleVal = resultSet.getDouble(colOrdinal);
          if(doubleVal != null && !resultSet.wasNull()) {
            sbConcatCols.append(doubleVal);
          }
          break;
        case Types.REAL:
          Float floatVal = resultSet.getFloat(colOrdinal);
          if(floatVal != null && !resultSet.wasNull()) {
            sbConcatCols.append(floatVal);
          }
          break;
        case Types.TINYINT:
          Short shortVal = resultSet.getShort(colOrdinal);
          if(shortVal != null && !resultSet.wasNull()) {
            sbConcatCols.append(shortVal);
          }
          break;
        case Types.BIGINT:
          Long longVal = resultSet.getLong(colOrdinal);
          if(longVal != null && !resultSet.wasNull()) {
            sbConcatCols.append(longVal);
          }
          break;
        case Types.DECIMAL:
          //DECIMAL is mapped to Spanner NUMERIC which does not store trailing zeros
          //Strip the trailing zeros from the source result before comparing
          BigDecimal decimalVal = new BigDecimal(resultSet.getBigDecimal(colOrdinal).stripTrailingZeros().toPlainString());
          if(decimalVal != null && !resultSet.wasNull()) {
            sbConcatCols.append(decimalVal);
          }
          break;
        case Types.DATE:
          Date dateVal = resultSet.getDate(colOrdinal);
          if(dateVal != null && !resultSet.wasNull()) {
            LocalDate localDate = dateVal.toLocalDate();
            sbConcatCols.append(String.format("%d%d%d",
                localDate.getYear(),
                localDate.getMonth().getValue(),
                localDate.getDayOfMonth()));
          }
          break;
        case Types.TIMESTAMP:
        case Types.TIME_WITH_TIMEZONE:
          // TODO: This uses millisecond precision; consider using microsecond precision
          java.sql.Timestamp timestampVal = resultSet.getTimestamp(colOrdinal);
          if(timestampVal != null && !resultSet.wasNull()) {
            Long rawTimestamp = timestampVal.getTime();
            if (adjustTimestampPrecision)
              rawTimestamp = rawTimestamp / 1000;
            sbConcatCols.append(rawTimestamp);
            if(timestampThresholdIndex >= 0) {
              if(i == timestampThresholdIndex) {
                retVal.timestampThresholdValue = rawTimestamp;
                if(adjustTimestampPrecision)
                  retVal.timestampThresholdValue = retVal.timestampThresholdValue * 1000;
              }
            }
          }
          break;
        default:
          LOG.error(String.format("Unsupported type: %d", type));
          throw new RuntimeException(String.format("Unsupported type: %d", type));
      } // switch
    } // for

    switch(rangeFieldType) {
      case TableSpec.UUID_FIELD_TYPE:
      case TableSpec.STRING_FIELD_TYPE:
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

  public static HashResult fromSourceRecord(SourceRecord sourceRecord,
      Integer keyIndex,
      String rangeFieldType,
      Boolean adjustTimestampPrecision,
      Integer timestampThresholdIndex) throws RuntimeException {

    HashResult retVal = new HashResult();
    StringBuilder sbConcatCols = new StringBuilder();
    for (int i = 0; i < sourceRecord.length(); i++) {
      Object colValue = sourceRecord.getField(i).getFieldValue();
      if (colValue == null) {
        continue;
      }
      String type = sourceRecord.getField(i).getFieldDataType();
      switch (type) {
        case "VARCHAR":
        case "CHAR":
        case "TEXT":
        case "UUID":
        case "BIT":
        case "BOOLEAN":
        case "INT":
        case "INTEGER":
        case "FLOAT":
        case "DOUBLE":
        case "BIGINT":
          sbConcatCols.append(colValue);
          break;
        case "JSONB":
        case "OTHER":
          sbConcatCols.append(getNormalizedJsonString(colValue.toString()));
          break;
        case "LONGVARBINARY":
        case "VARBINARY":
          sbConcatCols.append(Base64.encodeBase64String((byte[]) colValue));
          break;
        case "NUMERIC":
        case "DECIMAL":
          sbConcatCols.append(new BigDecimal(colValue.toString()).stripTrailingZeros().toPlainString());
          break;
        case "DATE":
          LocalDate localDate = ((java.sql.Date) colValue).toLocalDate();
          sbConcatCols.append(
              String.format("%d%d%d",
                  localDate.getYear(),
                  localDate.getMonth().getValue(),
                  localDate.getDayOfMonth())
          );
        case "TIMESTAMP":
          Timestamp timeStampVal = (Timestamp) colValue;
          Long rawTimestamp = timeStampVal.getTime();
          if (adjustTimestampPrecision) {
            rawTimestamp = rawTimestamp / 1000;
          }
          sbConcatCols.append(rawTimestamp);
          if(timestampThresholdIndex >= 0) {
            if(i == timestampThresholdIndex) {
              retVal.timestampThresholdValue = rawTimestamp;
            }
            if (adjustTimestampPrecision) {
              retVal.timestampThresholdValue = retVal.timestampThresholdValue * 1000;
            }
          }
          break;
        case "ARRAY":
          String[] vals = (String[]) colValue;
          for (String val : vals) {
            sbConcatCols.append(val);
          }
          break;
        default:
          LOG.error("Unsupported type: {}", type);
          throw new RuntimeException(String.format("Unsupported type: %s", type));
      }
    }

    switch(rangeFieldType) {
      case TableSpec.UUID_FIELD_TYPE:
      case TableSpec.STRING_FIELD_TYPE:
      case TableSpec.TIMESTAMP_FIELD_TYPE:
      case TableSpec.INT_FIELD_TYPE:
      case TableSpec.LONG_FIELD_TYPE:
        retVal.key = sourceRecord.getField(keyIndex).getFieldValue().toString();
        break;
      default:
        throw new RuntimeException(String.format("Unexpected range field type %s", rangeFieldType));
    }

    retVal.rangeFieldType = rangeFieldType;
    retVal.origValue = sbConcatCols.toString();
    retVal.sha256 = Helpers.sha256(retVal.origValue);
    retVal.isSource = true;
    LOG.info("SourceHash=> Key={}, OrigValue={} \n HashResult={}", retVal.key, retVal.origValue, retVal.sha256);
    return retVal;
  }

  private static String getNormalizedJsonString(String rawJsonInput) {
    return JSONNormalizer.getNormalizedString(rawJsonInput);
  }
} // HashResult