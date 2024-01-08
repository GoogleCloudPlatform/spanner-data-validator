package com.google.migration;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCToAvro {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCToAvro.class);
  private static final String JDBC_DRIVER = "org.postgresql.Driver";


  // [START jdbctoavro_options]
  public interface JDBCToAvroOptions extends PipelineOptions {

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
     * output location
     */
    @Description("output location")
    @Required
    String getOutputLocation();

    void setOutputLocation(String value);

    /**
     * Partition count
     */
    @Description("Partition count")
    @Default.Integer(100)
    Integer getPartitionCount();

    void setPartitionCount(Integer value);
  }
  // [END jdbctoavro_options]

  static void runMigrationToAvro(JDBCToAvroOptions options) throws IOException {
    Pipeline p = Pipeline.create(options);

    // JDBC conn string
    String connString = String.format("jdbc:postgresql://%s:%d/%s", options.getServer(),
        options.getPort(),
        options.getSourceDB());

    moveDataProductRecordsWithUUID(p, options, connString);
    moveDataProductMetadataWithUUID(p, options, connString);

    p.run().waitUntilFinish();
  }

  private static void moveDataProductRecordsWithUUID(Pipeline p,
      JDBCToAvroOptions options,
      String connString) throws IOException {

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

    dataProductRecords.setCoder(AvroCoder.of(GenericRecord.class, schema));

    dataProductRecords.apply("WriteToAvroDataProductRecords", AvroIO.writeGenericRecords(schema)
        .to(options.getOutputLocation() + "/product-records/records")
        .withSuffix(".avro"));
  }

  private static void moveDataProductMetadataWithUUID(Pipeline p,
      JDBCToAvroOptions options,
      String connString) throws IOException {

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

    dataProductMetadataRecords.setCoder(AvroCoder.of(GenericRecord.class, schema));

    dataProductMetadataRecords.apply("WriteToAvroDataProductMetadata", AvroIO.writeGenericRecords(schema)
        .to(options.getOutputLocation() + "/product-metadata/metadata")
        .withSuffix(".avro"));
  }

  public static void main(String[] args) throws IOException {
    JDBCToAvroOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(
            JDBCToAvroOptions.class);

    runMigrationToAvro(options);
  }
}