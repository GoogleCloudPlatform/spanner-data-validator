package com.google.migration;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretName;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.cloud.spanner.Type;
import com.google.migration.common.DVTOptionsCore;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Helpers {
  private static final Logger LOG = LoggerFactory.getLogger(Helpers.class);
  public static UUID bigIntToUUID(BigInteger in) {
    long low = in.longValue();
    long high = in.shiftRight(64).longValue();

    return new UUID(high, low);
  }

  public static BigInteger uuidToBigInt(UUID in) {
    // https://stackoverflow.com/questions/55752927/how-to-convert-an-unsigned-long-to-biginteger
    final BigInteger MASK_64 =
        BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    BigInteger msb = BigInteger.valueOf(in.getMostSignificantBits()).and(MASK_64).shiftLeft(64);
    BigInteger lsb = BigInteger.valueOf(in.getLeastSignificantBits()).and(MASK_64);
    BigInteger retVal = msb.or(lsb);

    return retVal;
  }

  public static List<KV<UUID, UUID>> getUUIDRangesWithCoverage(UUID start,
      UUID end,
      Integer partitionCount,
      Integer coveragePercent) {
    // UUID max
    BigInteger uuidMax = Helpers.uuidToBigInt(end);
    BigInteger uuidMin = Helpers.uuidToBigInt(start);
    BigInteger fullRange = uuidMax.subtract(uuidMin);
    BigInteger stepSize = fullRange.divide(BigInteger.valueOf(partitionCount.intValue()));

    // Simple implementation of "coverage" - just reduce the step size
    if(coveragePercent < 100) {
      stepSize = stepSize
          .divide(BigInteger.valueOf(100))
          .multiply(BigInteger.valueOf(coveragePercent));
    }

    ArrayList<KV<UUID, UUID>> bRanges = new ArrayList<>();

    // Account for first UUID
    bRanges.add(KV.of(start, start));

    BigInteger maxRange = uuidMin;
    for(Integer i = 0; i < partitionCount - 1; i++) {
      BigInteger minRange = maxRange;
      maxRange = minRange.add(stepSize);

      KV<UUID, UUID> range = KV.of(Helpers.bigIntToUUID(minRange), Helpers.bigIntToUUID(maxRange));

      bRanges.add(range);
    }

    KV<UUID, UUID> range = KV.of(Helpers.bigIntToUUID(maxRange), Helpers.bigIntToUUID(uuidMax));
    bRanges.add(range);

    return bRanges;
  }

  public static boolean isUUIDInRange(UUID valueToCheck, KV<UUID, UUID> range) {

    BigInteger rangeStart = uuidToBigInt(range.getKey());
    BigInteger rangeEnd = uuidToBigInt(range.getValue());
    BigInteger valueToCheckAsBigInt = uuidToBigInt(valueToCheck);

    int startCompareResult = valueToCheckAsBigInt.compareTo(rangeStart);
    if(startCompareResult < 0) return false;

    int endCompareResult = valueToCheckAsBigInt.compareTo(rangeEnd);
    if(endCompareResult >= 0) return false;

    return true;
  }

  public static KV<UUID, UUID> getRangeFromList(UUID lookup, List<KV<UUID, UUID>> rangeList) {
    Comparator<KV<UUID, UUID>> comparator = (o1, o2) -> {
      BigInteger lhs = Helpers.uuidToBigInt(o1.getKey());
      BigInteger rhs = Helpers.uuidToBigInt(o2.getKey());
      return lhs.compareTo(rhs);
    };

    List<KV<UUID, UUID>> sortedCopy = new ArrayList(rangeList);
    sortedCopy.sort(comparator);

    int searchIndex =
        Collections.binarySearch(sortedCopy, KV.of(lookup, lookup), comparator);

    int rangeIndex = searchIndex;
    if(searchIndex < 0) rangeIndex = -searchIndex - 2;

    return sortedCopy.get(rangeIndex);
  }

  public static Comparator<GenericRecord> getGenericRecordComparator() {
    Comparator<GenericRecord> comparator = (o1, o2) -> compareGenericRecords(o1, o2, false);

    return comparator;
  }

  public static boolean isNullOrEmpty(String stringToCheck) {
    if(stringToCheck == null || stringToCheck.trim().isEmpty()) {
      return true;
    }

    return  false;
  }



  public static GenericRecord jdbcResultSetToGenericRecord(String recordName,
      String schemaAsJson,
      ResultSet jdbcResultSet) throws SQLException {

    // TODO: optimize schema building (maybe pass it in)
    FieldAssembler<Schema> fields = SchemaBuilder
        .record(recordName).namespace("com.google.spannermigration.schemas")
        .fields();

    ResultSetMetaData rsMetaData = jdbcResultSet.getMetaData();
    int colCount = rsMetaData.getColumnCount();
    for(int i = 0; i < colCount; i++) {
      int colOrdinal = i+1;
      String colName = rsMetaData.getColumnName(colOrdinal);
      int type = rsMetaData.getColumnType(colOrdinal);

      // https://docs.oracle.com/javase/8/docs/api/constant-values.html
      // look for (java.sql.Types)
      switch (type) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.OTHER:
          fields = fields.name(colName).type().nullable().stringType().noDefault();
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          fields = fields.name(colName).type().nullable().booleanType().noDefault();
          break;
        case Types.INTEGER:
          fields = fields.name(colName).type().nullable().intType().noDefault();
          break;
        case Types.BIGINT:
        case Types.TIMESTAMP:
        case Types.TIME_WITH_TIMEZONE:
          fields = fields.name(colName).type().nullable().longType().noDefault();
          break;
        default:
          LOG.error(String.format("Unsupported type: %d", type));
          throw new RuntimeException(String.format("Unsupported type: %d", type));
      } // switch
    } // for

    Schema schema = fields.endRecord();
    GenericRecord record = new Record(schema);

    for(int i = 0; i < colCount; i++) {
      int colOrdinal = i+1;
      String colName = rsMetaData.getColumnName(colOrdinal);
      int type = rsMetaData.getColumnType(colOrdinal);

      // https://docs.oracle.com/javase/8/docs/api/constant-values.html
      // look for (java.sql.Types)
      switch (type) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.OTHER:
          record.put(colName, jdbcResultSet.getString(colOrdinal));
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          record.put(colName, jdbcResultSet.getBoolean(colOrdinal));
          break;
        case Types.INTEGER:
          record.put(colName, jdbcResultSet.getInt(colOrdinal));
          break;
        case Types.BIGINT:
          record.put(colName, jdbcResultSet.getLong(colOrdinal));
        case Types.TIMESTAMP:
        case Types.TIME_WITH_TIMEZONE:
          record.put(colName, jdbcResultSet.getTimestamp(colOrdinal).getTime());
          break;
        default:
          LOG.error(String.format("Unsupported type: %d", type));
          throw new RuntimeException(String.format("Unsupported type: %d", type));
      } // switch
    } // for

    return record;
  }

  public static GenericRecord spannerResultSetToGenericRecord(com.google.cloud.spanner.ResultSet spannerResultSet,
      String schemaStr) {
    Schema schema = new Schema.Parser().parse(schemaStr);
    GenericRecord record = new Record(schema);

    List<Schema.Field> fieldsList = schema.getFields();

    for(Field field: fieldsList) {
      String fieldName = field.name();
      Type columnType = spannerResultSet.getColumnType(fieldName);

      switch (columnType.toString()) {
        case "STRING":
          record.put(fieldName, spannerResultSet.getString(fieldName));
          break;
        case "INT64":
          record.put(fieldName, spannerResultSet.getLong(fieldName));
          break;
        case "TIMESTAMP":
          record.put(fieldName, spannerResultSet.getTimestamp(fieldName).toSqlTimestamp().getTime());
          break;
        case "BOOL":
        case "BOOLEAN":
          record.put(fieldName, spannerResultSet.getBoolean(fieldName));
          break;
        default:
          throw new RuntimeException(String.format("Unsupported type: %s", columnType));
      } // switch
    } // for

    return record;
  }

  public static int compareGenericRecords(GenericRecord lhs,
      GenericRecord rhs,
      boolean skipSchemaValidation) {
    Schema lhsSchema = lhs.getSchema();
    Schema rhsSchema = rhs.getSchema();

    if(!skipSchemaValidation) {
      int lhsColsCount = lhsSchema.getFields().size();
      int rhsColsCount = rhsSchema.getFields().size();

      if (lhsColsCount != rhsColsCount) {
        String errMsg = String.format("Schema cols don't match for %s and %s", lhsSchema.getName(),
            rhsSchema.getName());
        LOG.error(errMsg);
        LOG.error(String.format("LHS schema: %s, RHS schema: %s", lhsSchema, rhsSchema));
        throw new RuntimeException(errMsg);
      } // if
    } // if

    List<Schema.Field> fields = lhsSchema.getFields();
    int compareResult = -1;
    for(Schema.Field field: fields) {
      compareResult = compareGenericRecordField(field, lhs, rhs);
      if(compareResult != 0) return compareResult;
    } // for

    return compareResult;
  }

  private static int compareGenericRecordField(Schema.Field field,
      GenericRecord lhs,
      GenericRecord rhs) {
    Schema.Type type = field.schema().getType();
    String fieldName = field.name();

    if(type == Schema.Type.UNION) {
      type = field.schema().getTypes().get(0).getType();
    }

    switch (type) {
      case STRING:
        return lhs.get(fieldName).toString().compareTo(rhs.get(fieldName).toString());
      case INT:
        int lhsInt = (int)lhs.get(fieldName);
        int rhsInt = (int)rhs.get(fieldName);
        return Integer.compare(lhsInt, rhsInt);
      case BOOLEAN:
        boolean lhsBool = (boolean)lhs.get(fieldName);
        boolean rhsBool = (boolean)rhs.get(fieldName);
        return Boolean.compare(lhsBool, rhsBool);
      case LONG:
        long lhsLong = (long)lhs.get(fieldName);
        long rhsLong = (long)rhs.get(fieldName);
        return Long.compare(lhsLong, rhsLong);
      default:
        break;
    } // switch

    String errMsg = String.format("Unrecognized type: %s, field name: %s", type, fieldName);
    LOG.error(errMsg);
    throw new RuntimeException(errMsg);
  }

  public static String sha256(String originalString)  {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] encodedhash = digest.digest(
          originalString.getBytes(StandardCharsets.UTF_8));

      return Base64.encodeBase64String(encodedhash);
    } catch (Exception ex) {
      LOG.error(String
          .format("Error in Helpers.sha256: %s. Returning original string", ex));
      return originalString;
    }
  }

  public static String getSecret(String projectId, String secretId, String versionId) {
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretId, versionId);
      // Access the secret version.
      AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);

      byte[] data = response.getPayload().getData().toByteArray();
      Checksum checksum = new CRC32C();
      checksum.update(data, 0, data.length);
      if (response.getPayload().getDataCrc32C() != checksum.getValue()) {
        LOG.error("Data corruption detected while fetching secret");
        return null;
      }

      String payload = response.getPayload().getData().toStringUtf8();
      return payload;
    } // try
    catch (IOException e) {
      LOG.info("Error while attempting to get secret value");
      LOG.error(e.toString());
      LOG.error(e.getStackTrace().toString());
      throw new RuntimeException(e);
    } // try/catch
  }

  public static String getJDBCPassword(DVTOptionsCore options) {
    String pass = options.getPassword();
    String secretId = options.getDBPassFromSecret();
    if(!Helpers.isNullOrEmpty(secretId)) {
      String projectId = options.getProjectId();
      String secretVersion = options.getDBPassVersionForSecret();

      pass = getSecret(projectId, secretId, secretVersion);

      LOG.info("Using password from secret manager.");
    }

    return pass;
  }
} // class Helpers