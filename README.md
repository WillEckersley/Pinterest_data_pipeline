<p align="center">
    <img src="https://github.com/WillEckersley/Pintrest_data_pipeline/blob/main/README_images/icons8-pinterest-240.png" alt="Pinterest Logo">
</p>




# Pintrest_data_pipeline

## Introduction:

In this final project designed by AiCore, I constructed two big data pipelines. Each took raw data from Pintrest and transformed it using ELT processes. The intermediate results in both cases were cleaned datasets loaded into Databricks notebooks (see pintrest_batch_data_transformation.py and pintrest_stream_data_transformation.py in the databricks_notebooks folder in this repository). Ultimately, the batch processed data was subjected to analysis using SQL magic cells (for results see the notebook). In the case of the streamed dataset, the data was ultimately loaded directly into DeltaLake tables for permanent storage. As much of this project was executed in the cloud using a variety of AWS services, I have included an additional implementation_details.md file in this repository which provides further detail about how this project was constructed. In particular this documents the implementation of the cloud technologies which were used to create the project.

Building this project has exposed me to the use of big data tools in a production environment. It has strengthened my understanding of many of the tools that are crucual in the deployment of modern ELT pipelines such as Kafka, Airflow, datalakes (AWS S3), cloud computing (AWS EC2) and Databricks. 

## Pipeline Architechture:

### Batch processing:

<p align="center">
    <img src="https://github.com/WillEckersley/Pintrest_data_pipeline/blob/main/README_images/batch_data_diagram.svg">
</p>






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
