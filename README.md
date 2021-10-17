# Incubytes DE Task

## Overview

This repository contains implementation of given assessment. The working of data pipeline is demonstarted using tools & Technologies listed below. 
Also, a dummy database has been created to demonstarte a simple data flow in different formats from server to the local system, using country-based 
row filteration.

## Technologies & Tools

    Python
    pyspark
    Hive

## Concepts

    Data processing
    ETL

## Problem statement

    Data extracting will be done by our Source System. They will pull the all the relevant customer data 
    and will give us a Data file.
    
## Working

    Firstly in Hive database has been created with specified schema.
    Load sample data file in hive table with specific datatype.
    Import data from hive table.
    Filetering Date in proper format.
    Partitioning Data Contrywise and loading it into country table.
    checking the data format / describe
    Data Visualization (show data).
    filtering data from dataframe.
