#!/bin/bash

DATE=2024-05-20

# Création des dossiers HDFS
hdfs dfs -mkdir -p /raw/orders/$DATE/csv
hdfs dfs -mkdir -p /raw/orders/$DATE/parquet
hdfs dfs -mkdir -p /raw/stock/$DATE/csv
hdfs dfs -mkdir -p /raw/stock/$DATE/parquet

# Ingestion Orders
hdfs dfs -put orders/*.csv /raw/orders/$DATE/csv/
hdfs dfs -put orders/*.parquet /raw/orders/$DATE/parquet/

# Ingestion Stock
hdfs dfs -put stock/*.csv /raw/stock/$DATE/csv/
hdfs dfs -put stock/*.parquet /raw/stock/$DATE/parquet/

echo " Ingestion CSV et Parquet terminée pour la date $DATE"
