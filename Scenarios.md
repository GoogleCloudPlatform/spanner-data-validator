# CLI options for common scenarios

NOTE: These are sample values for options. Please replace them with your own!

NOTE: Sample tablespec json file [here](spanner-data-validator-java/src/main/resources/json/member-events-spec.json)

1. Kick off job with Direct Runner (local job)

```
mvn compile exec:java -Dexec.mainClass=com.google.migration.JDBCToSpannerDVTWithHash \
-Dexec.args="--protocol=mysql \
--server=localhost \
--port=3306 \
--username=<username> \
--password=<your password> \
--sourceDB=member_events_db \
--tableSpecJson=json/member-events-only-with-coverage-spec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--streaming=false \
" \
-Pdirect-runner
```

2. Kick of job on Dataflow (in the cloud)

NOTE: We recommend turning off autoscaling and manually configuring the number of workers based on the amount of data you expect to validate. This is why `--autoscalingAlgorithm=NONE` below.

```
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
--username=<username> \
--password=<your password> \
--sourceDB=member_events_db \
--tableSpecJson=json/member-events-spec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--streaming=false \
--autoscalingAlgorithm=NONE \
" \
-Pdataflow-runner
```

3. Kick off job on Dataflow with custom service account to be used by Dataflow

NOTE: We recommend turning off autoscaling and manually configuring the number of workers based on the amount of data you expect to validate. This is why `--autoscalingAlgorithm=NONE` below.

```
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
--username=<username> \
--password=<your password> \
--sourceDB=member_events_db \
--tableSpecJson=json/member-events-spec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--streaming=false \
--autoscalingAlgorithm=NONE \
--serviceAccount=SERVICE_ACCOUNT_NAME@PROJECT_ID.iam.gserviceaccount.com \
" \
-Pdataflow-runner
```

4. Kick off job on Dataflow with sharding spec

NOTE: We recommend turning off autoscaling and manually configuring the number of workers based on the amount of data you expect to validate. This is why `--autoscalingAlgorithm=NONE` below.

NOTE: When using the sharding spec, you have to supply the username and password in the sharding spec json file. See sample [here](spanner-data-validator-java/src/main/resources/json/shard-spec-sample-v1.json)

```
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
--username=<username> \
--password=<your password> \
--sourceDB=member_events_db \
--tableSpecJson=json/member-events-spec.json \
--tempLocation=gs://bigdata-stuff/df1 \
--projectId=kt-shared-project \
--instanceId=dvt-test1 \
--spannerDatabaseId=dvt-test1-db \
--streaming=false \
--autoscalingAlgorithm=NONE \
--serviceAccount=SERVICE_ACCOUNT_NAME@PROJECT_ID.iam.gserviceaccount.com \
" \
-Pdataflow-runner
```