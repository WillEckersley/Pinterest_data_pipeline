<p align="center">
    <img src="https://github.com/WillEckersley/Pintrest_data_pipeline/blob/main/README_images/icons8-pinterest-240.png" alt="Pinterest Logo">
</p>


# Pinterest_data_pipeline

#### Two big data ELT pipelines managing output from the popular website 'Pinterest'. 

## §1 Table of contents:

2. [Introduction](#§2-introduction)

3. [Pipeline Architecture](#§3-pipeline-architecture)
    
    3.1. [Batch Processing](#§3.1-batch-processing)

    &nbsp;&nbsp;&nbsp;&nbsp;3.1.1. [Extraction](#§3.1.1-extraction)
    
    &nbsp;&nbsp;&nbsp;&nbsp;3.1.2. [Loading](#§3.1.2-loading)
    
    &nbsp;&nbsp;&nbsp;&nbsp;3.1.3. [Transformation](#§3.1.3-transformation)

    &nbsp;&nbsp;&nbsp;&nbsp;3.1.4. [Orchestration](#§3.1.4-orchestration)

    3.2. [Stream Processing](#§3.2-stream-processing)

    &nbsp;&nbsp;&nbsp;&nbsp;3.2.1. [Extraction](#§3.2.1-extraction)

    &nbsp;&nbsp;&nbsp;&nbsp;3.2.2. [Loading](#§3.2.2-loading)

    &nbsp;&nbsp;&nbsp;&nbsp;3.2.3. [Transformation](#§3.2.3-transformation)

   
5. [Usage](#§4-usage)

    4.1 [Batch Data](#§4.1-batch-processing)

    4.2 [Stream Data](#§4.2-stream-processing)

6. [Technologies Used](#5-technologies-used)

    5.1 [Principal](#§5.1-principal)

    5.2 [Ancillary](#5.2-ancilliary)

7. [Installation](#6-installation)

## §2 Introduction:

In this final project designed by AiCore, I constructed two big data pipelines. Each took raw data from Pintrest and transformed it using ELT processes. The intermediate results in both cases were cleaned datasets loaded into Databricks notebooks (see pintrest_batch_data_transformation.py and pintrest_stream_data_transformation.py in the databricks_notebooks folder in this repository). Ultimately, the batch processed data was subjected to analysis using SQL magic cells (for results see the notebook).In the case of the streamed dataset, the data was ultimately loaded directly into DeltaLake tables for permanent storage. 

As much of this project was executed in the cloud using a variety of AWS services, I have included an additional implementation_details.md file in this repository which provides further detail about how this project was constructed. In particular, this documents the implementation of the cloud technologies which were used to create the project.

Building this project has exposed me to the use of big data tools in a production environment. It has strengthened my understanding of many of the tools that are crucial in the deployment of modern ELT pipelines such as Kafka, Airflow, datalakes (AWS S3), cloud computing (AWS EC2) and Databricks. 

## §3 Pipeline Architecture:

### §3.1 Batch processing:

<p align="center">
    <img src="https://github.com/WillEckersley/Pintrest_data_pipeline/blob/main/README_images/batch_data_diag.svg">
</p>

### §3.1.1 Extraction:

The data was extracted from an extant RDS database using a python script (user_posting_emulation_uploaded.py). This script simulates a stream of data coming from a website using a random row selector. The data came from three separate tables in the original RDS:

- pin (representing posts):

| Columns             | Datatypes |
|---------------------|-----------|
| ind                 | string   |
| unique ID           | string    |
| title               | string    |
| description         | string    |
| follower_count      | string    |
| poster_name         | string    |
| tag_list            | string    |
| is_image_or_video   | string    |
| image_src           | string    |
| save_location       | string    |
| category            | string    |
| downloaded          | string    |

  
- geo (representing the location of posts):

| Columns             | Datatypes |
|---------------------|-----------|
| ind                 | string    |
| country             | string    |
| latitude            | string    |
| longitude           | string    |
| timestamp           | string    |

- user (representing user data):

| Columns             | Datatypes |
|---------------------|-----------|
| ind                 | string    |
| first_name          | string    |
| last_name           | string    |
| age                 | string    |
| date_joined         | string    |

### §3.1.2 Loading:

This occurred in the following six steps: 

- Kafka topics corresponding to the three tables in the source RDS were created on an EC2 client machine with a preconfigured Kafka cluster.
- A REST proxy was created on API gateway and started on the EC2 client.
- The data was sent to REST proxy resource in the API.
- The Kafka cluster using MSK sent the data via its consumer to MSK connect.
- MSK connect was connected to an S3 bucket using a custom connector/plugin which directed the data to folders corresponding to Kafka topics within S3.
- The S3 bucket was mounted to Databricks.

Bellow: an image of the EC2 machine sending Kafka messages to the RESTful proxy.

![Image of kafka REST proxy receiving messages](https://github.com/WillEckersley/Pintrest_data_pipeline/blob/main/README_images/kafka_rest_recieving_screen.png)

See implementations_details.md in this repository for more details. 

### §3.1.3 Transformation:

Once in Databricks, the data was processed using a Spark cluster. First, it was cleaned. This invovled standard techniques of filtering, ordering etc (see pintrest_batch_data_transformation.py in this repository for details). As a result of the cleaning the following tables were created:

Post data (pin_df):

| Columns             | Datatypes |
|---------------------|-----------|
| ind                 | integer   |
| unique ID           | string    |
| title               | string    |
| description         | string    |
| follower_count      | integer   |
| poster_name         | string    |
| tag_list            | string    |
| is_image_or_video   | string    |
| image_src           | string    |
| save_location       | string    |
| category            | string    |
| downloaded          | boolean   |

Location data (geo_df):

| Columns             | Datatypes |
|---------------------|-----------|
| ind                 | integer   |
| country             | string    |
| coordinates         | array     |
| timestamp           | timestamp |

User data (user_df):

| Columns             | Datatypes |
|---------------------|-----------|
| ind                 | integer   |
| user_name           | string    |
| age                 | integer   |
| date_joined         | timestamp |

The data was then queried using Databricks SQL. The following insights were taken from the cleaned dataset:

-	The most popular Pinterest category in each country.
-	The number of posts in each category between 2018 and 2022.
-	The user with the most followers in each country.
-	The most popular category according to roughly generational age groups (18-24, 25-34 etc.)
-	The median follower count for the same age groups.
-	The number of users that have joined between 2015 and 2020
-	The median follower count for users that joined between 2015 and 2020.
-	The median follower count for users that joined between 2015 and 2020 based on their generational age grouping.

Again, please see pintrest_batch_data_processing.py in this repository for details.

### §3.1.4 Orchestration/management:

This took place using MWAA/Airflow. A DAG was created that sent a simple submit run to the Databricks notebook so it would be run once every day at 12 o'clock (see 12471ce1b695_dag.py in this repository).

Bellow: an image of the DAG orchestrating tasks via airflow.

![An image of the DAG orchestrating tasks via airflow.](https://github.com/WillEckersley/Pintrest_data_pipeline/blob/main/README_images/mwaa_screen.png)

### §3.2 Stream processing:

<p align="center">
    <img src="https://github.com/WillEckersley/Pintrest_data_pipeline/blob/main/README_images/kinesis_stream_diag.svg">
</p>

### §3.2.1 Extraction:

Again, the data was extracted from an extant RDS database using a python script (user_posting_emulation_streaming.py). As before, this script simulates a stream of data coming from a website using a random row selector. The data came from three separate tables in the original RDS:

- pin (representing posts)
- geo (representing the location of posts)
- user (representing user data)

### §3.2.2 Loading:

This occurred in a simpler fashion than the case of the batch data:

- 3 streams were created in Kinesis Datastreams, each corresponding to one of the tables in the original RDS.
- A RESTful API was constructed on API gateway with Kinesis Datastreams as a chosen endpoint.
- The Kinesis streams were mounted to a Databricks notebook.

See implementations_details.md in this repository for more details. 

### §3.2.3 Transformation:

Once in Databricks, the data was again processed using a Spark cluster. It was cleaned in the same fashion as before (see section 3.1.3 of this README) and then read to Delta tables using a custom function that also created table name specific checkpoints to allow for versioning.

Bellow: an image of the graphs monitoring the stream of data as it is written to DeltaTables. 

![An image of the graphs monitoring the stream of data as it is written to DeltaTables](https://github.com/WillEckersley/Pintrest_data_pipeline/blob/main/README_images/stream_screen.png)

## §4 Usage:

### §4.1 Batch data:

As previously mentioned, the batch data was queried using Databricks SQL once cleaned (see above for details of the query questions). Multiple usable business insights could be drawn from these queries. For instance, knowledge of the most popular categories in different geographic regions could be used to weight the posts displayed in those regions more heavily towards what is most popular there, thus prologuing site engagement. Knowledge of median follower counts over time could be used to determine the growth in Pintrest's reach across time according to age groupings.

### §4.2 Stream data

As the stream data was loaded directly to DeltaTables, it was intended for permanent storage. This would not exclude the possibility of it being used for querying in the future.

## §5 Technologies used:

### §5.1 Principal:

- AWS:
  - MSK
  - MSK Connect
  - EC2
  - S3
  - MWAA
  - Kinesis Data Streams
  - API Gateway
  - IAM

- Python
- Apache Spark
- Apache Kafka
- Databricks
- Databricks SQL
- Databricks DeltaLake Tables

### §5.2 Anciliary:

- SQL Alchemy
- Requests
- Boto3
- Json
- Multiprocessing
- Random
- Datetime
- Urllib

## §6 Installation:

To reproduce the environment needed to run the python files contained in this project use: 

```
git clone https://github.com/WillEckersley/Pintrest_data_pipeline
```
then run:

```
pip install -r requirements.txt
```

to reproduce the environment used to develop this project.

## §7 License:

Licensed under the Unlicense.
