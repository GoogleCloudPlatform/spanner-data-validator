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

## Running locally (with Maven)

mvn compile exec:java -Dexec.mainClass=com.google.migration.JDBCToSpannerDVTWithHash \
-Dexec.args="--protocol=mysql \
--server=localhost \
--port=3306 \
--username=kt_user \
--password=ktpas42* \
--sourceDB=member_events_db \
--tableSpecJson=src/main/resources/json/member-events-only-with-coverage-spec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--streaming=false \
" \
-Pdirect-runner

## Running locally (with java and compiled jar)

NOTE: This requires a previously compiled jar file; and please adjust the path to reflect the location of the jar in your environment.

java -jar target/spanner-data-validator-java-bundled-0.1.jar  \
--protocol=mysql \
--server=localhost \
--port=3306 \
--username=kt_user \
--password=ktpas42* \
--sourceDB=member_events_db \
--tableSpecJson=src/main/resources/json/member-events-only-with-coverage-spec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--BQDatasetName=SpannerDVTDataset \
--streaming=false \
--runner=DirectRunner

## Running on Dataflow

mvn compile exec:java -Dexec.mainClass=com.google.migration.JDBCToSpannerDVTWithHash \
-Dexec.args="--project=kt-shared-project \
--network=default \
--subnetwork=https://www.googleapis.com/compute/v1/projects/kt-shared-project/regions/us-central1/subnetworks/default \
--runner=DataflowRunner \
--numWorkers=10 \
--maxNumWorkers=20 \
--region=us-central1 \
--protocol=mysql \
--server=10.128.15.212 \
--port=3306 \
--username=kt_user \
--password=ktpas42* \
--sourceDB=member_events_db \
--tableSpecJson=json/member-events-spec.json \
--shardSpecJson=json/shard-spec-sample-v1.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--streaming=false \
" \
-Pdataflow-runner

## Running on Dataflow (with java and compiled jar)

NOTE: This requires a previously compiled jar file; and please adjust the path to reflect the location of the jar in your environment.

java -jar target/spanner-data-validator-java-bundled-0.1.jar  \
--project=kt-shared-project \
--network=default \
--subnetwork=https://www.googleapis.com/compute/v1/projects/kt-shared-project/regions/us-central1/subnetworks/default \
--numWorkers=10 \
--maxNumWorkers=20 \
--region=us-central1 \
--protocol=mysql \
--server=10.128.15.212 \
--port=3306 \
--username=kt_user \
--password=ktpas42* \
--sourceDB=member_events_db \
--tableSpecJson=src/main/resources/json/member-events-only-with-coverage-spec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--streaming=false \
--runner=DataflowRunner

## Running postgres locally (with java and compiled jar)

NOTE: This requires a previously compiled jar file; and please adjust the path to reflect the location of the jar in your environment.

java -jar target/spanner-data-validator-java-bundled-0.1.jar  \
--protocol=postgresql \
--server=localhost \
--port=5432 \
--username=kt_user \
--password=ktpas42* \
--sourceDB=kt_db \
--tableSpecJson=src/main/resources/json/postgres-uuid-tablespec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=tempus-test1 \
--spannerDatabaseId=tempus_db1 \
--BQDatasetName=SpannerDVTDataset \
--streaming=false \
--runner=DirectRunner

## Running postgres locally/secret manager (with java and compiled jar)

NOTE: This requires a previously compiled jar file; and please adjust the path to reflect the location of the jar in your environment.

java -jar target/spanner-data-validator-java-bundled-0.1.jar  \
--protocol=postgresql \
--server=localhost \
--port=5432 \
--username=kt_user \
--DBPassFromSecret=dbpass \
--sourceDB=kt_db \
--tableSpecJson=src/main/resources/json/postgres-uuid-tablespec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=tempus-test1 \
--spannerDatabaseId=tempus_db1 \
--BQDatasetName=SpannerDVTDataset \
--streaming=false \
--runner=DirectRunner

## Running postgres on Dataflow (with java and compiled jar)

NOTE: This requires a previously compiled jar file; and please adjust the path to reflect the location of the jar in your environment.

java -jar target/spanner-data-validator-java-bundled-0.1.jar  \
--project=kt-shared-project \
--network=default \
--subnetwork=https://www.googleapis.com/compute/v1/projects/kt-shared-project/regions/us-central1/subnetworks/default \
--numWorkers=10 \
--region=us-central1 \
--protocol=postgresql \
--server=10.128.0.24 \
--port=5432 \
--username=kt_user \
--password=ktpas42* \
--sourceDB=kt_db \
--tableSpecJson=src/main/resources/json/postgres-uuid-narrow-tablespec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=tempus-test1 \
--spannerDatabaseId=tempus_db1 \
--streaming=false \
--autoscalingAlgorithm=NONE \
--runner=DataflowRunner

## Reporting query samples

```sql
SELECT * FROM `kt-shared-project.SpannerDVTDataset.SpannerDVTResults` order by run_name desc LIMIT 1000

select run_name, sum(source_count), sum(target_count), sum(match_count), sum(source_conflict_count), sum(target_conflict_count)
from `kt-shared-project.SpannerDVTDataset.SpannerDVTResults`
group by run_name
order by run_name desc

SELECT count(1) FROM `kt-shared-project.SpannerDVTDataset.SpannerDVTResults` where run_name = 'Run-2024-01-31-09-50-15'
SELECT count(1) FROM `kt-shared-project.SpannerDVTDataset.SpannerDVTResults` where run_name = 'Run-2024-01-31-09-39-37'

SELECT distinct run_name FROM `kt-shared-project.SpannerDVTDataset.SpannerDVTResults` order by run_name desc
```