# YouTube Trending Data Analysis with PySpark

## Context
With the continuous growth of digital platforms, massive datasets are increasingly common and require distributed processing tools to be analyzed efficiently.  
This project is part of an academic assignment focused on **Big Data analysis using Apache Spark**, applied to a large-scale YouTube Trending Videos dataset covering **113 countries** and more than **2 million records** :contentReference[oaicite:0]{index=0}.

Apache Spark was used to process, clean, and analyze this dataset in a distributed environment deployed with **Hadoop and Docker**.

---

## Problem Statement
The main challenge of this project is to **process and analyze a very large CSV dataset (~2.9 GB)** that cannot be handled efficiently with traditional single-machine tools.

Key issues addressed:
- Correctly reading **multi-line CSV data** (YouTube descriptions spanning multiple lines)
- Cleaning and structuring raw data for reliable analysis
- Performing scalable transformations and aggregations using **Spark DataFrames and RDDs**
- Extracting **meaningful business insights** from massive data volumes

---

## Project Description
This project analyzes YouTube trending videos data using **PySpark**, focusing on data cleaning, transformation, aggregation, and performance comparison between **DataFrame API** and **RDD API**.

### Main steps:
- Loading the dataset from **HDFS** into Spark
- Handling multi-line CSV parsing and schema inspection
- Data cleaning (null values, type conversion)
- Transformations using Spark DataFrames:
  - Selection and filtering of relevant columns
  - Calculation of engagement metrics (likes / views)
  - Aggregations by country and date
  - Ranking and TOP-N analysis
- Actions such as counting, aggregation, and writing results to **Parquet format**
- Re-implementing key analyses using **RDDs** to compare approaches

---

## Business Questions Addressed
- Which countries generate the highest number of trending videos?
- Which countries have the highest average engagement rate?
- How many videos exceed **1 million views**, and how does this vary over time?
- What are the most viewed trending videos globally?

---

## Technologies Used
- **Apache Spark (PySpark)**
- **Hadoop HDFS**
- **Docker**
- **Python**
- **Parquet format**

---

## Notes
This repository is a **learning and practice project** created for academic purposes, focusing on hands-on experience with Big Data processing using Spark.
