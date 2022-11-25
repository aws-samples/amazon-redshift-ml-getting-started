# Getting Started with Redshift Streaming Ingestion

In this repo, we’ll showcase some popular use-cases using [Amazon Redshift Streaming Ingestion](hhttps://docs.aws.amazon.com/redshift/latest/dg/materialized-view-streaming-ingestion.html) and how to get started with it.

## Introduction

Data engineers, data analysts, and big data developers are using real-time streaming engines to improve customer responsiveness. With the new streaming ingestion capability in Amazon Redshift, you can use SQL (Structured Query Language) to connect to and directly ingest data from multiple Kinesis data streams simultaneously. Amazon Redshift streaming ingestion simplifies data pipelines by letting you create materialized views on top of streams directly. The materialized views can also include SQL transformations as part of your ELT (Extract Load Transform) pipeline. You can manually refresh defined materialized views to query the most recent stream data. This approach allows you to perform downstream processing and transformations of streaming data using existing familiar tools at no additional cost.

## Repository Overview

In this repository, we have showcase setting up Amazon Redshift Streaming Ingestion using [Amazon kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/) and [Fully Managed Streaming for Apache Kafka](https://aws.amazon.com/msk/)

We have also provided a [Cloudformation Template](./cloud-formation/cloud-formation-template.yaml) where applicable. 

## What is Redshift Streaming Ingestion

### What is Streaming Data?

In simple terms, streaming data is data that is generated continuously and simultaneously sent to another application to process the data. Streaming data includes a wide variety of data such as log files from mobile or web applications, ecommerce purchases, in-game player activity, information from social networks, financial trading floors, or geospatial services, POS systems, clickstream data and telemetry from IOT devices or instrumentation in data centers.

### What is Kinesis Data Streams?
Amazon Kinesis Data Streams is a scalable and durable real-time data streaming service that can continuously capture, buffer and optionally analyze gigabytes of data per second from hundreds of thousands of sources and make it available to applications that can analyze the data. 

### What is Kafka/MSK?
Apache Kafka is an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications. Managed Streaming for Kafka (MSK) is an AWS managed Apache Kafka platform. MSK allows customers to get up and running with Kafka without the need to manage resources or patching

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.