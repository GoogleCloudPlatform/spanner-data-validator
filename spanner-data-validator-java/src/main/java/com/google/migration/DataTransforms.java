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

package com.google.migration;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTransforms {

  private static final Logger LOG = LoggerFactory.getLogger(DataTransforms.class);

  public static class DTRowMapper implements RowMapper<GenericRecord> {
    private SerializableFunction<ResultSet, GenericRecord> internalMapper;

    public DTRowMapper(SerializableFunction<ResultSet, GenericRecord> internalMapperIn) {
      internalMapper = internalMapperIn;
    }

    @Override
    public GenericRecord mapRow(@UnknownKeyFor @NonNull @Initialized ResultSet resultSet) {
      return internalMapper.apply(resultSet);
    }
  }

  public static org.apache.avro.Schema getDataProductRecordsAvroSchema() throws IOException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    InputStream is = classloader.getResourceAsStream("avro/data_product_record.avsc");
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(is);

    return schema;
  }

  public static RowMapper<GenericRecord> getProductRecordsMapper()
      throws IOException {
    org.apache.avro.Schema schema = getDataProductRecordsAvroSchema();
    SerializableFunction<ResultSet, GenericRecord> internalMapper =
        new ProductRecordFromResultSetFn(schema.toString());
    RowMapper<GenericRecord> mapper = new DTRowMapper(internalMapper);

    return mapper;
  } // getProductRecordsMapper

  public static org.apache.avro.Schema getDataProductMetadataAvroSchema() throws IOException {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    InputStream is = classloader.getResourceAsStream("avro/data_product_metadata.avsc");
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(is);

    return schema;
  }

  public static class ProductRecordFromResultSetFn implements SerializableFunction<ResultSet, GenericRecord> {

    private transient org.apache.avro.Schema schema;
    private String schemaString;

    public ProductRecordFromResultSetFn(String schemaStringIn) {
      schemaString = schemaStringIn;
    }

    @Override
    public GenericRecord apply(ResultSet resultSet) {

      if(schema == null) {
        schema = new org.apache.avro.Schema.Parser().parse(this.schemaString);
      }

      GenericRecord avroRecord = new GenericData.Record(schema);

      try {
        // ResultSet has 1 based indexing
        avroRecord.put("id", resultSet.getString(1));
        avroRecord.put("type", resultSet.getString(2));
        avroRecord.put("bucket", resultSet.getString(3));
        avroRecord.put("key", resultSet.getString(4));
        // TODO: consider Instant
        avroRecord.put("last_modified", resultSet.getTimestamp(5).getTime());
        avroRecord.put("recorded_at", resultSet.getTimestamp(6).getTime());
        avroRecord.put("version_id", resultSet.getString(7));
        avroRecord.put("gcs_bucket", resultSet.getString(8));
        avroRecord.put("gcs_key", resultSet.getString(9));
        avroRecord.put("gcs_confirmed", resultSet.getBoolean(10));
        avroRecord.put("version", resultSet.getString(11));
        avroRecord.put("extension", resultSet.getString(12));
        avroRecord.put("record_id", resultSet.getString(13));
        avroRecord.put("created_at", resultSet.getTimestamp(14).getTime());

        return avroRecord;
      } catch (Exception ex) {

        LOG.error(ex.getMessage());
        LOG.error(ex.getStackTrace().toString());

        return null;
      }
    }
  }

  public static class GenericRecordFromResultSetFn
      implements SerializableFunction<ResultSet, GenericRecord> {

    private transient org.apache.avro.Schema schema;
    private String schemaString;
    private String schemaName;

    public GenericRecordFromResultSetFn(String schemaStringIn, String schemaNameIn) {
      schemaString = schemaStringIn;
      schemaName = schemaNameIn;
    }

    @Override
    public GenericRecord apply(ResultSet resultSet) {
      try {
        return Helpers.jdbcResultSetToGenericRecord(schemaName, schemaString, resultSet);
      } catch (Exception ex) {

        LOG.error(ex.getMessage());
        LOG.error(ex.getStackTrace().toString());

        return null;
      } // try/catch
    }
  } // class GenericRecordFromResultSetFn

  public static class ProductMetadataFromResultSetFn implements SerializableFunction<ResultSet, GenericRecord> {

    private transient org.apache.avro.Schema schema;
    private String schemaString;

    public ProductMetadataFromResultSetFn(String schemaStringIn) {
      schemaString = schemaStringIn;
    }

    @Override
    public GenericRecord apply(ResultSet resultSet) {
      try {
        // GenericRecord recordHC = hardCodedTransform(resultSet);
        // GenericRecord recordD = dynamicTransform(resultSet);
        //
        // LOG.info(String.format("Hard coded record: %s, Dynamic record: %s", recordHC, recordD));
        //
        // return recordD;
        return dynamicTransform(resultSet);
      } catch (Exception ex) {

        LOG.error(ex.getMessage());
        LOG.error(ex.getStackTrace().toString());

        return null;
      } // try/catch
    }

    public GenericRecord dynamicTransform(ResultSet resultSet) throws SQLException {

      return Helpers.jdbcResultSetToGenericRecord("DataProductMetadata", schemaString, resultSet);
    }
  }

  public static RowMapper<GenericRecord> getProductMetadataMapper()
      throws IOException {

    org.apache.avro.Schema schema = getDataProductMetadataAvroSchema();
    SerializableFunction<ResultSet, GenericRecord> internalMapper =
        new ProductMetadataFromResultSetFn(schema.toString());
    RowMapper<GenericRecord> mapper = new DTRowMapper(internalMapper);

    return mapper;
  } // getProductMetadataMapper
} // class DataTransforms