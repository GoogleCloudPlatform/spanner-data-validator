// Copyright 2023 Google LLC
//
//     Licensed under the Apache License, Version 2.0 (the "License");
//     you may not use this file except in compliance with the License.
//     You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

package com.google.migration;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Default.Enum;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToSpanner {

  private static final String JDBC_DRIVER = "org.postgresql.Driver";
  private static final Logger LOG = LoggerFactory.getLogger(JDBCToSpanner.class);

  public static class RowToGenericRecordDoFn extends DoFn<Row, GenericRecord> {

    Schema schema;

    @Setup
    public void setup() throws IOException {
      ClassLoader classloader = Thread.currentThread().getContextClassLoader();
      InputStream is = classloader.getResourceAsStream("avro/data_product_record.avsc");
      schema = new Schema.Parser().parse(is);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      GenericRecord avroRecord = new GenericData.Record(schema);
      avroRecord.put("id", c.element().getValue(0).toString());
      avroRecord.put("type", c.element().getString(1));
      avroRecord.put("bucket", c.element().getString(2));
      avroRecord.put("key", c.element().getString(3));
      avroRecord.put("last_modified", ((Instant) c.element().getValue(4)).getMillis());
      avroRecord.put("recorded_at", ((Instant) c.element().getValue(5)).getMillis());
      avroRecord.put("version_id", c.element().getString(6));
      avroRecord.put("gcs_bucket", c.element().getString(7));
      avroRecord.put("gcs_key", c.element().getString(8));
      avroRecord.put("gcs_confirmed", c.element().getBoolean(9));
      avroRecord.put("version", c.element().getString(10));
      avroRecord.put("extension", c.element().getString(11));
      avroRecord.put("record_id", c.element().getValue(12).toString());
      avroRecord.put("created_at", ((Instant) c.element().getValue(13)).getMillis());
      c.output(avroRecord);
    }
  }

  public static class MyRow implements Serializable {

    private String Id;

    public String getId() {
      return Id;
    }

    public void setId(String id) {
      Id = id;
    }

    private String Type;

    public String getType() {
      return Type;
    }

    public void setType(String type) {
      Type = type;
    }
  }


  // [START jdbctospanner_options]
  public interface JDBCToSpannerOptions extends PipelineOptions {

    /**
     * The JDBC server.
     */
    @Description("JDBC Server")
    @Default.String("localhost")
    String getServer();

    void setServer(String value);

    /**
     * The source DB.
     */
    @Description("Source DB")
    @Required
    String getSourceDB();

    void setSourceDB(String value);

    /**
     * The JDBC port
     */
    @Description("JDBC Port")
    @Default.Integer(5432)
    Integer getPort();

    void setPort(Integer value);

    /**
     * Username.
     */
    @Description("Username")
    @Required
    String getUsername();

    void setUsername(String value);

    /**
     * Password
     */
    @Description("Password")
    @Required
    String getPassword();

    void setPassword(String value);

    /**
     * ProjectId
     */
    @Description("ProjectId")
    @Required
    String getProjectId();

    void setProjectId(String value);

    /**
     * InstanceId
     */
    @Description("InstanceId")
    @Required
    String getInstanceId();

    void setInstanceId(String value);

    /**
     * Destination DatabaseId
     */
    @Description("Destination DatabaseId")
    @Required
    String getSpannerDatabaseId();

    void setSpannerDatabaseId(String value);

    @Description("Dialect of the database that is used")
    @Default
    @Enum("POSTGRESQL")
    // alternative: GOOGLE_STANDARD_SQL
    Dialect getDialect();

    void setDialect(Dialect dialect);

    /**
     * Partition count
     */
    @Description("Partition count")
    @Default.Integer(100)
    Integer getPartitionCount();

    void setPartitionCount(Integer value);
  }
  // [END jdbctospanner_options]

  static void runMigrationToSpanner(JDBCToSpannerOptions options) throws IOException {
    Pipeline p = Pipeline.create(options);

    // JDBC conn string
    String connString = String.format("jdbc:postgresql://%s:%d/%s", options.getServer(),
        options.getPort(),
        options.getSourceDB());

    Write sWrite = getSpannerWrite(p, options);

    moveDataProductRecordsWithUUID(p, options, connString, sWrite);
    moveDataProductMetadataWithUUID(p, options, connString, sWrite);

    p.run().waitUntilFinish();
  }

  private static Write getSpannerWrite(Pipeline p, JDBCToSpannerOptions options) {

    Write sWrite = SpannerIO.write()
        .withProjectId(options.getProjectId())
        .withInstanceId(options.getInstanceId())
        .withDatabaseId(options.getSpannerDatabaseId());

    if(options.getDialect() == Dialect.POSTGRESQL) {
      PCollectionView<Dialect> dialectView =
          p.apply(Create.of(Dialect.POSTGRESQL)).apply(View.asSingleton());
      sWrite.withDialectView(dialectView);
      LOG.info("Setting POSTGRESQL dialect");
    }

    return sWrite;
  }

  private static void moveDataProductRecordsWithUUID(Pipeline p,
      JDBCToSpannerOptions options,
      String connString,
      Write sWrite) throws IOException {

    List<KV<UUID, UUID>> bRanges = Helpers.getUUIDRanges(options.getPartitionCount());

    // LOG.info(String.format("Range start: %s, range end: %s", Helpers.bigIntToUUID(maxRange), Helpers.bigIntToUUID(uuidMax)));

    PCollection<KV<UUID, UUID>> pRanges = p.apply(Create.of(bRanges));

    String query = "select * from \"data-products\".data_product_records where id > uuid(?) and id <= uuid(?)";

    Schema schema = DataTransforms.getDataProductRecordsAvroSchema();

    // https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.html
    PCollection<GenericRecord> dataProductRecords = pRanges.apply("ReadFromDataProductRecordsInParallel",
        JdbcIO.<KV<UUID, UUID>, GenericRecord>readAll()
            .withDataSourceConfiguration(DataSourceConfiguration.create(
                    JDBC_DRIVER, connString)
                .withUsername(options.getUsername())
                .withPassword(options.getPassword()))
            .withQuery(query)
            .withParameterSetter(Helpers.getPreparedStatementSetterForUUID())
            .withRowMapper(DataTransforms.getProductRecordsMapper())
            .withOutputParallelization(false)
            .withCoder(AvroCoder.of(GenericRecord.class, schema))
    );

    dataProductRecords.apply("CreateSpannerMutationsForDataProductRecords", ParDo.of(new DoFn<GenericRecord, Mutation>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(Mutation.newInsertOrUpdateBuilder("data_product_records")
                .set("id").to(c.element().get(0).toString())
                .set("type").to(c.element().get(1).toString())
                .set("bucket").to(c.element().get(2).toString())
                .set("key").to(c.element().get(3).toString())
                .set("last_modified").to(com.google.cloud.Timestamp.of(new Date((Long)c.element().get(4))))
                .set("recorded_at").to(com.google.cloud.Timestamp.of(new Date((Long)c.element().get(5))))
                .set("version_id").to(c.element().get(6).toString())
                .set("gcs_bucket").to(c.element().get(7).toString())
                .set("gcs_key").to(c.element().get(8).toString())
                .set("gcs_confirmed").to((Boolean) c.element().get(9))
                .set("version").to(c.element().get(10).toString())
                .set("extension").to(c.element().get(11).toString())
                .set("record_id").to(c.element().get(12).toString())
                .set("created_at").to(com.google.cloud.Timestamp.of(new Date((Long)c.element().get(13))))
                .build());
          }
        }))
        .apply("WriteDataProductRecords", sWrite);
  }

  private static void moveDataProductMetadataWithUUID(Pipeline p,
      JDBCToSpannerOptions options,
      String connString,
      Write sWrite) throws IOException {

    List<KV<UUID, UUID>> bRanges = Helpers.getUUIDRanges(options.getPartitionCount());

    // LOG.info(String.format("Range start: %s, range end: %s", Helpers.bigIntToUUID(maxRange), Helpers.bigIntToUUID(uuidMax)));

    PCollection<KV<UUID, UUID>> pRanges = p.apply(Create.of(bRanges));

    String query = "select * from \"data-products\".data_product_metadata where data_product_id > uuid(?) and data_product_id <= uuid(?)";

    Schema schema = DataTransforms.getDataProductMetadataAvroSchema();

    // https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/jdbc/JdbcIO.html
    PCollection<GenericRecord> dataProductMetadataRecords = pRanges.apply("ReadFromDataProductMetadataInParallel",
        JdbcIO.<KV<UUID, UUID>, GenericRecord>readAll()
            .withDataSourceConfiguration(DataSourceConfiguration.create(
                    JDBC_DRIVER, connString)
                .withUsername(options.getUsername())
                .withPassword(options.getPassword()))
            .withQuery(query)
            .withParameterSetter(Helpers.getPreparedStatementSetterForUUID())
            .withRowMapper(DataTransforms.getProductMetadataMapper())
            .withOutputParallelization(false)
            .withCoder(AvroCoder.of(GenericRecord.class, schema))
    );

    dataProductMetadataRecords.apply("CreateSpannerMutationsForDataProductMetadata", ParDo.of(new DoFn<GenericRecord, Mutation>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws SQLException {

            c.output(Mutation.newInsertOrUpdateBuilder("data_product_metadata")
                .set("key").to(c.element().get(0).toString())
                .set("value").to(c.element().get(1).toString())
                .set("data_product_id").to(c.element().get(2).toString())
                //.set("jsonb_field").to(Value.pgJsonb("{\"rating\":9,\"open\":true}"))
                .set("jsonb_field").to("{\"rating\":9,\"open\":true}")
                .build());
          }
        }))
        .apply("WriteDataProductMetadata", sWrite);
  }

  public static void main(String[] args) throws IOException {
    JDBCToSpannerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(JDBCToSpannerOptions.class);

    runMigrationToSpanner(options);
  }
}