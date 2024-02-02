# Pintrest_data_pipeline
Final project for AiCore - Big Data pipeline

## Pipeline Architechture:

## Usage

### Phase 1: Cloud services implementation:
#### 1. EC2 and IAM

#### 2. Installing Kafka and configuring the EC2 client.  
The client was accessed using SSH having first generated the requisite permissions file by retrieving key:pair values from the parameter store.
Once the EC2 instance was created and configured, the first major implementation was downloading and installing Apache Kafka on the instance: 

```
wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
tar -xzf kafka_2.12-2.8.1.tgz
```

As the EC2 was protected using IAM, there was a need to download and configure the IAM MSK authentication package: https://github.com/aws/aws-msk-iam-auth:

```
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.5/aws-msk-iam-auth-1.1.5-all.jar
```

This package authenticates MSK usage on EC2 instances which require IAM authentication. It was installed in kafka_2.12-2.8.1/libs (the Kafka installation folder) and then set to the variable CLASSPATH in the EC2’s .bashrc file using:

```
CLASSPATH=/home/ec2-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-1.1.5-all.jar
```

This implementation was configured by navigating to the kafka_2.12-2.8.1/bin/ folder and creating a client.properties file containing:

```
# Sets up TLS for encryption and SASL for authN.
security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="<My IAM Access Role>";

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Having retrieved the Bootstrap Servers String and the Plaintext Apache Zookeeper connection string from the MSK console, 3 topics were created; one for each of the dataframes generated in the landing zone (Databricks Notebook). 

```
/kafka-topics.sh --bootstrap-server BootstrapServerString --command-config client.properties --create --topic <topic_name>
```

#### 3. Creating the datalake source for the landing zone. 
An S3 bucket was created for use as a datalake source. This received a largely default configuration. MSK connect was then deployed to establish a connection between the Kafka implementation on the EC2 and the S3 datalake. This required configuration on both the EC2 machine and through the AWS console. 

On the EC2 side, confluent.io Amazon S3 connector was used. This implementation was a matter of downloading the requisite .zip file to a new folder (kafka-connect-s3) in the EC2 instance then exporting it to the S3 bucket. 

```
sudo -u ec2-user -i
mkdir kafka-connect-s3 && cd kafka-connect-s3
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip
aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<BUCKET_NAME>/kafka-connect-s3/
```

A custom plugin then needed to be created through the MSK connect AWS console. Here the plugin object field was filled with the location of the confluent.io .zip file:
*Screen*

A corresponding connection was then created in order to establish the pipeline connection between the EC2 client and the S3 datalake. The connector was created through the AWS console and received the following configuration:

```
connector.class=io.confluent.connect.s3.S3SinkConnector
s3.region=us-east-1
flush.size=1
schema.compatibility=NONE
tasks.max=3
topics.regex=<YOUR_UUID>.*
format.class=io.confluent.connect.s3.format.json.JsonFormat
partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
value.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
storage.class=io.confluent.connect.s3.storage.S3Storage
key.converter=org.apache.kafka.connect.storage.StringConverter
s3.bucket.name=<BUCKET_NAME>
```

Additionally:
- Connector type was changed to Provisioned.
- MCU count per worker and Number of workers were set to 1.
- Under Worker Configuration, the confluent-worker custom configuration was selected.
- Under access permissions, the EC2 IAM role used for accessing the instance was selected as the access method. 

#### 4. Creating an API gateway REST proxy to establish a connection to EC2 client. 

Using API gateway in the AWS console, a {/proxy+} resource was created. Within this an ANY resource was provisioned with HTTP proxy integration. The EC2’s DNS address was naturally chosen as the endpoint of the integration. To ensure that the EC2 was accessible, it was then necessary to download and install the Confluent REST proxy on the EC2 client:

```
sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz
tar -xvzf confluent-7.2.0.tar.gz 
```
 
Within the resulting folder (confluent.7.2.0), the kafka-rest.properties file received the following configuration:

```
# Sets up TLS for encryption and SASL for authN.
client.security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
client.sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
client.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="<My IAM Acces Role>";

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
client.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

In order to test the pipeline, the EC2 client was started by navigating to the confluent7.2.0/bin/ folder ann running:

```
./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties
```

Test data was then sent via the API to the EC2. This was successful. At this stage a python file (user_posting_emulation.py - viewable in the repository) was executed which would send the mock streaming data to the API to which the EC2 instance was connected. This data landed successfully in the requisite S3 bucket topics at a test stage of the API. The API resource was restaged to run in 'prod' and the python emulation file process was left to run for a number of hours in order for data to build up in the topics ready for cleaning and analysis. 

### Phase 2: cleaning, analysis and scheduled processing. 

#### 1.Mounting the S3 datalake into databricks for cleaning and analysis. 

To the view the process of mounting an S3 bucket to Databricks, please see the mount_s3.ipynb file in the notebooks folder in this repository. This involves a sho
rt series of commands to load this data. Once this is done, it need not be run again. The file in this repository is included as evidence of the process. 

#### 2. Cleaning:
Dataframes corresponding to each of the topics in the datalake were created. The data in them was then cleaned via the deployment of Apache Spark functions. Primary tasks included:

- Replacement of erroneous/meaningless data with ‘None’.
- Recasting of data types.
- Removal of unnecessary repeated string data e.g. ‘Save location:’.
- Reordering and renaming of columns.
- Construction of an array column for longitude/latitude location data.
- Joining of separate first and last name columns to create a username column.

Full details of the implementation of this process can be viewed in the pintrest_batch_processing.ipynb file in this repository.

#### 3. Analysis

The cleaned dataframes were then analysed using a series of Databricks SQL queries. To view the code, please refer to the notebook pintrest_batch_processing.ipynb. The analysis aimed at generating a series of business insights: 

-	The most popular Pinterest category in each country.
-	The number of posts in each category between 2018 and 2022.
-	The user with the most followers in each country.
-	The most popular category according to roughly generational age groups (18-24, 25-34 etc.)
-	The median follower count for the same age groups
-	The number of users that have joined between 2015 and 2020
-	The median follower count for users that joined between 2015 and 2020
-	The median follower count for users that joined between 2015 and 2020 based on their generational age grouping.

A number of SQL methods were deployed to reach these results including joins, CTEs, cases and window functions. 

#### 4. Scheduling 
A DAG was created (see 12471ce1b695_dag.py in this repository) to schedule processing workloads every day at midday. This file was uploaded to an S3 bucket within the MWAA environment. This was manually triggered upon creation to test it’s efficacy. The test was successful: 
*Screen*

## Technologies used:

### Principal:

- AWS (MSK, MSK Connect, EC2, S3, MWAA, Kinesis)
- Python
- Apache Spark
- Apache Kafka
- Databricks
- Databricks SQL

### Anciliary:

- SQL Alchemy
- Requests

