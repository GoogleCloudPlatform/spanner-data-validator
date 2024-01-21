# Readme for Spanner DVT (with hashing)

Spanner DVT is a Dataflow based application that can be used to validate data between Postgres and Spangres. At high level,
1. The application uses Beam's JDBCIO to perform partitioned reads from Postgres
2. It then groups the results by partition range. For each range, it queries Spangres
3. And within a group, it compares the results from Postgres and Spangres.

## Specifying partitions

The application expects the user to specify the following via the TableSpec type in code

1. The source (Postgres) query and the target (Spangres) query
2. The type of the range field (only UUID and Timestamp are currently supported)
3. The index of the range field in the AVRO schema (see below)
4. The coverage (as a percentage) across the range field type space

Example:

```java
    TableSpec spec = new TableSpec(
        "DataProductMetadata",
        "select * from \"data-products\".data_product_metadata where data_product_id > uuid(?) and data_product_id <= uuid(?)",
        "SELECT key, value, data_product_id FROM data_product_metadata "
            + "WHERE data_product_id > $1 AND data_product_id <= $2",
        2,
        100,
        TableSpec.UUID_FIELD_TYPE
    );
```

## Running locally

mvn compile exec:java -Dexec.mainClass=com.google.migration.JDBCToSpannerDVTWithHash \
-Dexec.args="--protocol=mysql \
--server=localhost \
--port=3306 \
--username=kt_user \
--password=ktpas42* \
--sourceDB=member_events_db \
--supportShardedSource=false \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--streaming=false \
" \
-Pdirect-runner

## Running on Dataflow

mvn compile exec:java -Dexec.mainClass=com.google.migration.JDBCToSpannerDVTWithHash \
-Dexec.args="--project=kt-shared-project \
--network=default \
--subnetwork=https://www.googleapis.com/compute/v1/projects/kt-shared-project/regions/us-central1/subnetworks/default \
--runner=DataflowRunner \
--numWorkers=10 \
--maxNumWorkers=20 \
--region=us-central1 \
--server=10.128.0.24 \
--port=5432 \
--username=kt_user \
--password=ktpas42* \
--sourceDB=kt_db \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=tempus-test1 \
--spannerDatabaseId=tempus_db1 \
" \
-Pdataflow-runner

## Potential improvements/ TODOs (not in priority order)
1. Read tablespec from Json (in GCS?)
2. Fine tuning of reporting schema and data (BQ)
3. General cleanup and reorg/refactor
4. BigQueryIO write method (use direct write)