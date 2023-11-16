# Museum-Data-Pipeline

This is a project to produce a live dashboard to help visualise trends in ratings given to a museum's exhibitions, as well as analysing frequencies of assistance and emergency requests at the museum. The dashboard will help to provide insights which in turn allow staff at the museum to better allocate resources and improve existing exhibitions.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)

## Overview

Visitors of the museum can rate an exhibition from 0 to 4, with 4 being the highest rate, or alternatively, can request for assistance or an emergency at an exhibition. Initally, the data used was static and downloaded from an s3 bucket, whereas the finished project consumes live data from a Kafka cluster. The data is sent through a pipeline where here, the data is cleaned and used to populate the relevant tables within a database. The database is linked to Tableau and is used to construct several visualisations to better understand any trends within the data.

## Features

1. **Real-time Data**
   - Data collected from the Kafka cluster is in real-time. Exactly once a second, the consume.py script checks the Kafka broker for any new published messages, and if so, consumes and parses this data.

2. **Information Logging**
   - At each stage of the pipeline, messages are logged to a separate file indicating the success of each stage, or otherwise indicating that an error has occured, coupled with the error message itself.
  
3. **Optional Error Logging**
   - An optional command-line argument can be passed when running the kafka_to_db.py script, which if included, logs any data entries that cause errors and therefore not suitable for database entry, as well as the reason why the data contains an error.
  
## Usage

1. ```schema.sql```
   
    - Run this script in any database engine to create the database tables and establish any relationships between them. Throughout the project, I opted to use PostgreSQL.

2. ```extract.py```
   
    - Downloads all files from an s3 bucket and filters through the files to remove any irrelevant files. Files remaining contain data referring to information about the museum exhibitions, or data about votes, emergencies, or assistance requests. These are further combined into two separate CSV files in preparation for initially populating the database and the pipeline respectively.

3. ```populate_db_tables.py```
   
    - From the data collected on the exhibitions in step 2, populates the database with information on each exhibition. Moreover, information about each department and the meaning of each vote rating is added to the database now. This data is added separately to the actual vote data for two reasons: (a) this data is subject change infrequently; (b) separation here makes for better readability.

4. ```pipeline.py```
   
    - Connects to the database, and passes the static vote data stored in the combined csv file through the pipeline and thereafter inserts it into the database.
  
5. ```kafka_to_db.py```
   
    - Connects to the database, and passes the live vote data from the kafka cluster through the pipeline and thereafter inserts it into the database.
   
## Disclaimer

The Kafka cluster nor the data stored on s3 are currently publicly available and so it is not possible to run either data source through the pipeline. 
