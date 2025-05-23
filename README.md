# Spanner Data Validator

This repo contains sample implementations of data validation for Spanner databases using Apache Beam. Since [Spanner](https://cloud.google.com/spanner) is a horizontally scalable database that can scale pretty much without limit, we need a framework for data validation that can handle this scale. [Apache Beam](https://beam.apache.org/) provides just such a scalable framework. It supports multiple languages - Java, Python, Go, etc..., but we use Java in this repo. While you can run Apache Beam applications locally on your machine or individual instances in the cloud, in order to realize the full power of the framework and scale across *multiple* instances, you have to run your application/job on a service like Dataflow. I describe how to do this in the section titled [Running validation using Dataflow](#running-validation-using-dataflow) below.

Having said all of the above, I should mention that you don't have to know much about Apache Beam or Dataflow or be a Java developer to use this tool. Simply follow the guidance below to get your own validation against Spanner up and running! You can jump to the [Installation](#installation) section and pick up from there if you'd prefer to skip the following section on the design of the tool.

## Design

First, here's a high level architecture diagram of how our data validator will work:

![High level arch diagram](arch-diagrams/high-level-arch-diagram.png "High level arch diagram")

We use Apache Beam running on Dataflow to perform our data validation at scale and then send the reports to BigQuery. The reporting itself is pretty straightforward. Here's a sample report:


The above sample report provides a window into the design of this data validator. Let's start with the notion of partitioning.

### Partitioning

Partitioning is key to performing data validation at scale. The approach is simple: we partition data into chunks and for each chunk, issue the same query at the source and target. Here’s a sample query (for both source and target)

```
select * from customers where customerNumber > ? and customerNumber <= ?
```

And thanks to partitioning, this query will be run in parallel across Dataflow workers (units of parallelism to be more accurate). The results of the query will be used to perform comparisons and results of the comparisons in turn will be sent to BigQuery (see reporting section below).

## Assumptions

1. The column name used in the partition query should be the same in source and target and should be an indexed column with very high cardinality.
2. ``SELECT X `` relies on column ordering and both source and target have the same column. It is fairly easy to get a SELECT clause as input to the tool where the user ensures the same order and also casting to ensure the same outcome between source and target even if the column type is different.

## Prerequisites

1. Access to a GCP account
2. Access to Spanner (Since you'll be performing validation against Spanner :))
3. Access to BigQuery 
4. Access to Dataflow
5. Ensure that you have [JRE 11](https://docs.oracle.com/goldengate/1212/gg-winux/GDRAD/java.htm) installed on your machine.
6. Ensure that you have a MySQL or PostgreSQL source database (or databases in case of sharding) available to run the tool against.
7. Create or identify a pre-existing dataset in BQ that you can use for reporting purposes. The validation tool writes the results of the validation to this BigQuery dataset. The validation tool will create a table called ‘SpannerDVTResults’ in this dataset into which it will write the results.
8. Request the permissions listed in the section below

## Permissions

1. User should have permissions to kick off Dataflow jobs
2. Default Dataflow service agent should permissions to read/write from buckets: See [link](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#df-service-account)
3. Dataflow worker account (default is compute engine service account) should have
   - Ability to create tables in specified dataset: `roles/bigquery.dataEditor`
   - Ability to write to the specified dataset: ` roles/bigquery.dataEditor`
   - Read from Spanner: `roles/spanner.databaseReader`
   - Read from JDBC (MySQL or Postgresql)
   - If using Secret Manager to supply password, the dataflow worker service account should have `roles/secretManager.secretAccessor` and `roles/secretManager.secretViewer`

## Cloning the tool

`spanner-data-validator` uses [git-lfs](https://git-lfs.com/) to store a JAR
that is required to run custom transformations. Users will need to set up and 
use `git-lfs` to download the JAR file while cloning this repository. Follow
the instructions below - 

1. Clone the repository.
2. Install `git-lfs` using instructions [here](https://docs.github.com/en/articles/installing-git-large-file-storage).  
3. From the `spanner-data-validator` git repository, run `git lfs pull`.
## Building the tool

NOTE: Ensure that you're in the `spanner-data-validator-java/` folder before running the following command

```json
mvn validate
```

NOTE: `mvn validate` installs required dependencies to your maven cache for the
build process to work, so don't forget to run this command!. It only needs
to be run **once** to setup the maven cache successfully. 

```
mvn clean package -Pdirect-and-dataflow -DskipTests
```

## Running Validations

NOTE: In the examples below, we’re using a pre-built jar so you don’t have to run mvn (Maven) commands.

### Simple (non-sharded) local validation

```
java -jar target/spanner-data-validator-java-bundled-0.1.jar  \
--protocol=postgresql \
--server=localhost \
--port=3306 \
--username=<your db username> \
--DBPassFromSecret=<secretId> \
--sourceDB=member_events_db \
--tableSpecJson=src/main/resources/json/member-events-only-with-coverage-spec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--BQDatasetName=SpannerDVTDataset \
--streaming=false \
--runner=DirectRunner

```

Parameters

- `protocol`: `postgresql` or `mysql`
- `server`: DB host
- `port`: DB port
- `username`: DB username
- `DBPassFromSecret`: GCP secret manager entry containing DB password
- `sourceDB`: Source DB
- `tableSpecJson`: Table spec json file. See [sdfkj](fosd) for more info
- `tempLocation`: GCS location for holding staging info
- `projectId`: This is project id used for Spanner, BQ and Secret Manager
- `instanceId`: Spanner instance id
- `spannerDatabaseId`: Spanner database id
- `BQDatasetName`: BQ dataset where report will be written
- `streaming`: False here because this will be a batch job
- `runner`: In this case, Direct Runner, to reflect a local run

### Running (non-sharded) validation on Dataflow

```
java -jar target/spanner-data-validator-java-bundled-0.1.jar  \
--project=kt-shared-project \
--network=default \
--subnetwork=https://www.googleapis.com/compute/v1/projects/kt-shared-project/regions/us-central1/subnetworks/default \
--numWorkers=10 \
--region=us-central1 \
--protocol=postgresql \
--server=10.128.15.212 \
--port=3306 \
--username=<your mysql db user> \
--DBPassFromSecret=<secretId> \
--sourceDB=member_events_db \
--tableSpecJson=src/main/resources/json/member-events-only-with-coverage-spec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--BQDatasetName=SpannerDVTDataset \
--conflictingRecordsBQTableName=<Name of table to write unmatched records>\
--streaming=false \
--autoscalingAlgorithm=NONE \
--runner=DataflowRunner
```

Parameters

- `protocol`: `postgresql` or `mysql`
- `server`: DB host
- `port`: DB port
- `username`: DB username
- `DBPassFromSecret`: GCP secret manager entry containing DB password
- `sourceDB`: Source DB
- `tableSpecJson`: Table spec json file. See [sdfkj](fosd) for more info
- `tempLocation`: GCS location for holding staging info
- `projectId`: This is project id used for Spanner, BQ and Secret Manager
- `instanceId`: Spanner instance id
- `spannerDatabaseId`: Spanner database id
- `BQDatasetName`: BQ dataset where report will be written
- `streaming`: False here because this will be a batch job
- `runner`: In this case, Direct Runner, to reflect a local run

## Sample TableSpec json files

You specify tables/queries for validation using the TableSpec json files.

### Sample w/ 2 tables

```
[
 {
   "tableName": "DataProductRecords",
   "sourceQuery": "select * from \"data-products\".data_product_records where id >= uuid(?) and id <= uuid(?)",
   "destQuery": "select * FROM data_product_records WHERE id >= $1 AND id <= $2",
   "rangeFieldIndex": "0",
   "rangeFieldType": "UUID",
   "rangeStart": "00000000-0000-0000-0000-000000000000",
   "rangeEnd": "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
   "partitionCount": "100",
   "rangeCoverage": "0.00000000002"
 },
 {
   "tableName": "DataProductMetadata",
   "sourceQuery": "select * from \"data-products\".data_product_metadata where data_product_id >= uuid(?) and data_product_id <= uuid(?)",
   "destQuery": "select key, value, data_product_id FROM data_product_metadata WHERE data_product_id >= $1 AND data_product_id <= $2",
   "rangeFieldIndex": "2",
   "rangeFieldType": "UUID",
   "rangeStart": "00000000-0000-0000-0000-000000000000",
   "rangeEnd": "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF",
   "partitionCount": "100",
   "rangeCoverage": "0.00000000001"
 }
]
```

### Custom transformations

Custom transformations allow the user to inject a JAR which implements the
[spanner-migrations-sdk](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/spanner-migrations-sdk)
interface to augment the validation logic. This
functionality is useful to pass the same custom transformation JAR what might
have been used in other data migration tools, such as [source-to-spanner
dataflow template](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/sourcedb-to-spanner/README_Sourcedb_to_Spanner.md)
or [datastream-to-spanner dataflow template](https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/datastream-to-spanner/README_Cloud_Datastream_to_Spanner.md).

#### Assumptions

When custom transformations are enabled (that is, the relevant flags are
configured), all non-common columns on the spanner that are part of the session
file will be assumed under custom transformation.

#### Pre-requisites

1. Build the JAR for spanner-data-validator using
   instructions [here](#building-the-tool).
2. Build the JAR for your custom transformation as per the
   instructions [here](https://googlecloudplatform.github.io/spanner-migration-tool/custom-transformation).
3. Using a session file is mandatory for using custom transformations.
4. The column under custom transformation should be part of the supplied session
   file.

#### Configuring custom transformations in validations

Custom transformation support requires configuring identical parameters to bulk,
live and reverse replication dataflow jobs. This includes
`transformationJarPath`,
`transformationClassName` and `transformationCustomParameters`. A sample command
is
given below -

```shell
java -jar target/spanner-data-validator-java-bundled-0.1.jar  \
--project=sample-project \
--network=sample-vpc \
--subnetwork=https://www.googleapis.com/compute/v1/projects/span-cloud-testing/regions/us-central1/subnetworks/aks-test-vpc \
--numWorkers=10 \
--region=us-central1 \
--protocol=mysql \
--server=10.36.112.26 \
--port=3306 \
--username=username \
--password=pwd \
--sourceDB=validation_test \
--sessionFileJson=src/main/resources/json/custom_transform.json \
--tempLocation=gs://sample-bucket/validation-testing \
--projectId=sample-project \
--instanceId=sample-instance \
--spannerDatabaseId=sample-database \
--BQDatasetName=SpannerDVTDataset \
--transformationJarPath=gs://sample-bucket/test/smt_util-0.0.2-SNAPSHOT.jar \
--transformationClassName=com.example.test.smtutil.spanner.Transformer \
--transformationCustomParameters=table.default.abc=colName1,table.list=colName2 \
--runner=DataflowRunner
```
