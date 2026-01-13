# Procurement Data Pipeline

## Project Overview
This project implements a simplified batch-oriented procurement data pipeline for an online grocery retailer.
The goal is to compute daily supplier orders based on customer demand, warehouse stock levels, and business rules
such as safety stock, minimum order quantity (MOQ), and pack size.

The project follows a pedagogical distributed architecture using HDFS, PostgreSQL, Trino, and Python.

---

## Architecture Overview

The pipeline is composed of the following components:

- **HDFS**: Acts as a Data Lake to store batch data and final outputs.
- **PostgreSQL (OLTP)**: Stores reference data and staging tables.
- **Trino (OLAP)**: Executes distributed analytical SQL queries.
- **Python**: Used only as an orchestration layer.

All analytical computations are executed by Trino.

---

## Data Sources

### 1. Customer Orders (Batch)
- Daily customer orders collected from multiple centers (stores or delivery zones).
- Represented using a staging table (`tmp_orders`).
- Each record contains the order date, center identifier, product SKU, and quantity sold.

### 2. Warehouse Stock Snapshots
- End-of-day inventory snapshots stored in a staging table (`tmp_stock`).
- Includes available stock and reserved stock for each SKU and center.
- Used to calculate the net available stock.

### 3. Reference Data (OLTP)
Stored in PostgreSQL as stable reference tables:
- `suppliers`: List of suppliers.
- `products`: Product information including supplier, MOQ, and pack size.
- `safety_stock`: Safety stock thresholds per SKU.

---

## Analytical Processing (OLAP)

Trino is used as the main analytical engine to:

1. Aggregate customer orders per product and center.
2. Compute net stock by subtracting reserved stock from available stock.
3. Integrate safety stock thresholds.
4. Apply business rules (MOQ and pack size).
5. Calculate the final quantity to order per supplier.

Hive Metastore is intentionally not used due to configuration complexity in lightweight Docker environments.

---

## Orchestration with Python

A Python script is used to orchestrate the pipeline by:
- Connecting to Trino.
- Executing analytical SQL queries.
- Fetching results.
- Generating one CSV file per supplier.
- Storing the output files in HDFS.

Python does not perform any analytical computation.

---

## Output

The final output is a set of CSV files stored in HDFS:

