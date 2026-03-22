# Procurement Data Pipeline

## Overview
This project implements a batch-oriented data pipeline designed to compute daily procurement needs for an online grocery retailer.

It transforms raw operational data (orders, stock levels, and business constraints) into actionable supplier orders using a distributed data architecture.

> Original project: https://github.com/SALMA-BENAAMMI/procurement-data-pipeline.git

---

## Objective
The goal is to determine the optimal quantity to order for each supplier by combining:
- Customer demand
- Warehouse stock availability
- Business constraints (safety stock, MOQ, pack size)

---

## Architecture

The pipeline follows a simplified modern data architecture:

- **HDFS** → Data Lake for batch storage and outputs  
- **PostgreSQL** → OLTP system for reference and staging data  
- **Trino** → Distributed SQL engine for analytical processing  
- **Python** → Orchestration layer  

All heavy computations are performed in **Trino**, ensuring scalability and separation of concerns.

---

## Data Sources

### Customer Orders
- Daily batch data from multiple centers  
- Stored in `tmp_orders`  
- Contains product, location, and quantity  

### Stock Snapshots
- End-of-day inventory data  
- Stored in `tmp_stock`  
- Includes available and reserved stock  

### Reference Data (PostgreSQL)
- `products` → product details, supplier, MOQ, pack size  
- `suppliers` → supplier information  
- `safety_stock` → minimum stock thresholds  

---

## Data Processing (OLAP with Trino)

The analytical pipeline includes:

1. Aggregation of daily demand per SKU and location  
2. Computation of net stock  
3. Integration of safety stock constraints  
4. Application of business rules (MOQ & pack size)  
5. Calculation of final supplier order quantities  

---

## Orchestration

A Python script is used to:
- Execute SQL queries in Trino  
- Retrieve results  
- Generate supplier-specific CSV files  
- Store outputs in HDFS  

⚠️ Python is used strictly for orchestration, not computation.

---

## Output

- CSV files per supplier  
- Stored in HDFS  
- Ready for downstream procurement processes  

---

## Collaboration

This project was developed collaboratively in a team of three.

- Full participation across all stages of the pipeline  
- Collaborative design and implementation  
- Shared responsibilities in data processing, modeling, and orchestration  

---

## Key Learnings

- Designing end-to-end data pipelines  
- Working with distributed systems (HDFS, Trino)  
- Separation between OLTP and OLAP systems  
- Applying real-world business constraints in data workflows  
- Collaborative development in data engineering projects  

---

## Technologies

- Python  
- SQL (Trino)  
- PostgreSQL  
- HDFS  

---
