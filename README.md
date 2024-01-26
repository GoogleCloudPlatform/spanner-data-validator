# Spanner Data Validator

This repo contains sample implementations of data validation for Spanner databases using Apache Beam. Since [Spanner](https://cloud.google.com/spanner) is a horizontally scalable database that can scale pretty much without limit, we need a framework for data validation that can handle this scale. [Apache Beam](https://beam.apache.org/) provides just such a scalable framework. It supports multiple languages - Java, Python, Go, etc..., but we use Java in this repo. While you can run Apache Beam applications locally on your machine or individual instances in the cloud, in order to realize the full power of the framework and scale across *multiple* instances, you have to run your application/job on a service like Dataflow. I describe how to do this in the section titled [Running validation using Dataflow](#running-validation-using-dataflow) below.

Having said all of the above, I should mention that you don't have to know much about Apache Beam or Dataflow or be a Java developer to use this tool. Simply follow the guidance below to get your own validation against Spanner up and running! You can jump to the [Installation](#installation) section and pick up from there if you'd prefer to skip the following section on the design of the tool.

## Design

First, here's a high level architecture diagram of how our data validator will work:

![High level arch diagram](arch-diagrams/high-level-arch-diagram.png "High level arch diagram")

We use Apache Beam running on Dataflow to perform our data validation at scale and then send the reports to BigQuery. The reporting itself is pretty straightforward. Here's a sample report:


The above sample report provides a window into the design of this data validator. Let's start with the notion of partitioning.

### Partitioning

Partitioning is key to performing data validation at scale.

## Prerequisites

1. Access to a GCP account
2. Access to Spanner (Since you'll be performing validation against Spanner :))
3. Access to BigQuery 
4. Access to Dataflow

NOTE: See section below for specific permissions

## Permissions

1. User should have permissions to kick off Dataflow jobs
2. Default Dataflow service agent should permissions to read/write from buckets: See [link](https://cloud.google.com/dataflow/docs/concepts/security-and-permissions#df-service-account)
3. Dataflow worker account (default is compute engine service account) should have
   - Ability to create tables in specified dataset
   - Ability to write to the specified dataset
   - Read from Spanner
   - Read from JDBC (MySQL or Postgres)

## BigQuery datasets

In order to perform reporting, the validator expects you supply a BigQuery Dataset (has to exist) and Table name (will be created if it doesn't exist).

## Connectivity

1. The machine from which you're kicking off the Dataflow job must have connectivity to the MySQL/Postgres database(s) and the Spanner database. This is true whether you intend for the comparison job to run locally or on Dataflow.
2. When running on Dataflow, the Dataflow workers must be able to talk to the MySQL/Postgres database(s) and the Spanner database.

## Installation

Please refer to the [installation instructions](Installation.md) to get started.

## Common scenarios

See [CLI options for common scenarios](Scenarios.md)

## Supported configurations

1. JDBC <=> Spanner (MySQL and PostgreSQL)
2. Spanner <=> Spanner