# 🧩 Azure Data Engineering Project: End-to-End ETL Pipeline

This project showcases a complete **ETL pipeline** using **Microsoft Azure**, modeled on the **Medallion Architecture (Bronze → Silver → Gold)**. It demonstrates real-world **data engineering practices**, leveraging the Azure ecosystem to ingest, transform, and serve data for analytical and business insights.

---

## 🎯 Project Objective

Build a scalable Azure-based pipeline that:

- Ingests structured data from GitHub
- Cleans & transforms it using Azure Databricks (PySpark)
- Stores data across **Bronze, Silver, and Gold layers** in ADLS Gen2
- Enables querying via Synapse Serverless SQL
- Visualizes insights with Power BI

---

## 🧬 Architecture Overview

```plaintext
[GitHub CSV Files]
        |
        v
Azure Data Factory (Ingestion Pipeline)
        |
        v
Azure Data Lake Storage Gen2
├── Bronze: Raw data (as-is)
├── Silver: Cleaned & enriched data (Databricks + PySpark)
└── Gold: Final analytical tables
        |
        v
Azure Synapse Serverless SQL (External Tables)
        |
        v
Power BI Dashboard (Business Insights)
Special thanks to Ansh Lamba for the detailed masterclass that helped bring this project to life.

👨‍💻 Author & Contact
Author: Mohd Ahsan Khan
🔗 LinkedIn Profile
📂 Project Repository
