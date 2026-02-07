Sales Analysis Dashboard with Hadoop MapReduce
Project Overview

This project demonstrates a Big Data processing pipeline using Hadoop MapReduce to analyze large-scale sales data and a Streamlit dashboard to visualize the results.

The objective is to process a large CSV dataset containing sales transactions, compute key business indicators using distributed MapReduce jobs, store the results in HDFS, and present them through an interactive web dashboard.

Architecture

The project is based on a Dockerized Hadoop cluster composed of:

One Master node running NameNode and ResourceManager

Two Worker nodes running DataNodes and NodeManagers

HDFS for distributed storage

Hadoop Streaming with Python for MapReduce jobs

Streamlit for data visualization

Docker and Docker Compose for deployment

Dataset

File name: ventes_big_data_final.csv

Storage location: HDFS (/user/root/input)

The dataset contains sales transactions with information such as:

Country

Date (Year-Month)

Transaction amount

Payment method

Returns and refunds

MapReduce Jobs

Four MapReduce jobs were implemented using Hadoop Streaming and Python:

Net Revenue by Country and Month
Computes the net revenue by aggregating sales and subtracting returns.
Output directory: output_ca

Top 10 Transactions
Identifies the top 10 highest sales transactions.
Output directory: output_top10

Returns Analysis
Aggregates returned amounts by country and month.
Output directory: output_retours

Payments Analysis
Aggregates sales amounts by payment method.
Output directory: output_paiements

All outputs are stored in HDFS and merged into local CSV files for visualization.

Dashboard

The Streamlit dashboard provides:

Stacked bar charts showing net revenue by country and month

Detailed tables with Country and Month breakdowns

Clear visualization of business indicators computed by MapReduce

The dashboard runs inside the Hadoop master container and is accessible at:

http://localhost:8501
