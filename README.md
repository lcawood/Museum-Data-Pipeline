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
## Installation
## Usage
