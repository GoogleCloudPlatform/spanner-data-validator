# Readme for Spanner DVT

Spanner DVT is a Dataflow based application that can be used to validate data between JDBC and Spanner (both Google SQL and PostgreSQL). At high level,
1. The application uses Beam's JDBCIO to perform partitioned reads from Postgres
2. It then groups the results by partition range. For each range, it queries Spanner
3. And within a group, it compares the results from Postgres and Spangres.

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
--partitionCount=100 \
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
--protocol=mysql \
--server=10.128.15.212 \
--port=3306 \
--username=kt_user \
--password=ktpas42* \
--sourceDB=member_events_db \
--supportShardedSource=true \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--streaming=false \
" \
-Pdataflow-runner

## Important topics (in no particular order)

### Installing pre-requisites

1. Install JDK 11
2. Install Maven

### Permissions

Ensure that the Dataflow worker service account (typically the compute engine service account) has the following permissions

- Database reader for Spanner (the specific DB being validated)
- GCS read permissions (bucket used for temp/staging)
- Write permissions to BQ dataset

### Specifying partitions

The application expects the user to specify the following via the TableSpec type in code

1. The source (Postgres/MySQL) query and the target (Spangres) query
2. The type of the range field (only UUID, int32 and int64 are currently supported)
3. The coverage (as a percentage) across the range field type space

### BigQuery for reporting

blah blah

### Specifying coverage

blah blah

### Estimating the number of workers

Don't just rely on auto-scaling. Based on a rough estimate of how much data you think will be processed by the validation job, allocate Dataflow VMs appropriately.

Blah blah.

### Coming up with number of partitions

blah blah

### Use max(key column) to constrain overall partition range

blah blah

### Listing queries

blah blah

### Sharding

blah blah

### Table spec

- Query nuances
- LIMIT clause (NO!)

### Protocol for JDBC

MySQL/Postgres

## Troubleshooting

### Running on Dataflow - connectivity

Ensure that the machine from which you're kicking off the Dataflow job can talk to both the source (MySQL/Postgres) *and* Spanner

### Invalid key type

blah blah

### Unsupported type

blah blah