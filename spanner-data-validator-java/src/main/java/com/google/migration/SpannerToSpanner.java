package com.google.migration;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerToSpanner {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSpanner.class);

  // [START spannertospanner_options]
  public interface SpannerToSpannerOptions extends PipelineOptions {

    /**
     * SourceProjectId
     */
    @Description("SourceProjectId")
    @Required
    String getSourceProjectId();

    void setSourceProjectId(String value);

    /**
     * SourceInstanceId
     */
    @Description("SourceInstanceId")
    @Required
    String getSourceInstanceId();

    void setSourceInstanceId(String value);

    /**
     * Source DatabaseId
     */
    @Description("Source DatabaseId")
    @Required
    String getSourceDatabaseId();

    void setSourceDatabaseId(String value);

    /**
     * Source Table
     */
    @Description("Source Table")
    @Default
    @Default.String(("data_product_records"))
    String getSourceTable();

    void setSourceTable(String value);

    @Description("Dialect of the source database")
    @Default
    @Default.Enum("POSTGRESQL")
      // alternative: GOOGLE_STANDARD_SQL
    Dialect getSourceDialect();

    void setSourceDialect(Dialect dialect);

    /* destination */

    /**
     * DestProjectId
     */
    @Description("DestProjectId")
    @Required
    String getDestProjectId();

    void setDestProjectId(String value);

    /**
     * DestInstanceId
     */
    @Description("DestInstanceId")
    @Required
    String getDestInstanceId();

    void setDestInstanceId(String value);

    /**
     * Dest DatabaseId
     */
    @Description("Dest DatabaseId")
    @Required
    String getDestDatabaseId();

    void setDestDatabaseId(String value);

    /**
     * Dest Table
     */
    @Description("Dest Table")
    @Default
    @Default.String(("data_product_records"))
    String getDestTable();

    void setDestTable(String value);

    @Description("Dialect of the dest database")
    @Default
    @Default.Enum("POSTGRESQL")
      // alternative: GOOGLE_STANDARD_SQL
    Dialect getDestDialect();

    void setDestDialect(Dialect dialect);
  }
  // [END spannertospanner_options]

  static void runMigration(SpannerToSpannerOptions options) throws IOException {
    Pipeline p = Pipeline.create(options);

// Query for all the columns and rows in the specified Spanner table
    // table 1
    PCollection<Struct> records = p.apply(
        SpannerIO.read()
            .withProjectId(options.getSourceProjectId())
            .withInstanceId(options.getSourceInstanceId())
            .withDatabaseId(options.getSourceDatabaseId())
            .withQuery("SELECT * FROM " + options.getSourceTable()));
            //.withQuery("SELECT * FROM data_product_records limit 10"));
            //.withQuery("SELECT * FROM blah "));

    Write sWrite = SpannerIO.write()
        .withProjectId(options.getDestProjectId())
        .withInstanceId(options.getDestInstanceId())
        .withDatabaseId(options.getDestDatabaseId());

    if(options.getDestDialect() == Dialect.POSTGRESQL) {
      PCollectionView<Dialect> dialectView =
          p.apply(Create.of(Dialect.POSTGRESQL)).apply(View.asSingleton());
      sWrite.withDialectView(dialectView);
    }

    // https://github.com/GoogleCloudPlatform/java-docs-samples/blob/85e60ed9e2b6666c8920201caad2ef1f8a46849f/dataflow/spanner-io/src/main/java/com/example/dataflow/SpannerWrite.java#L189
    records.apply("CreateSpannerMutations", ParDo.of(new DoFn<Struct, Mutation>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            LOG.info(String.format("Element %s", c.element().getString(0)));
            c.output(Mutation.newInsertOrUpdateBuilder("data_product_records")
                .set("id").to(c.element().getValue(0).toString())
                .set("type").to(c.element().getString(1))
                .set("bucket").to(c.element().getString(2))
                .set("key").to(c.element().getString(3))
                .set("last_modified").to(c.element().getTimestamp(4))
                .set("recorded_at").to(c.element().getTimestamp(5))
                .set("version_id").to(c.element().getString(6))
                .set("gcs_bucket").to(c.element().getString(7))
                .set("gcs_key").to(c.element().getString(8))
                .set("gcs_confirmed").to(c.element().getBoolean(9))
                .set("version").to(c.element().getString(10))
                .set("extension").to(c.element().getString(11))
                .set("record_id").to(c.element().getValue(12).toString())
                .set("created_at").to(c.element().getTimestamp(13))
                .build());
          }
        }))
        .apply("WriteProductRecords", sWrite);

    p.run().waitUntilFinish();
  }

  public static void main(String[] args) throws IOException {
    SpannerToSpannerOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerToSpannerOptions.class);

    runMigration(options);
  }
}
