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


🛠️ Tools & Technologies

| Tool/Service             | Purpose                                          |
| ------------------------ | ------------------------------------------------ |
| **Azure Data Factory**   | Data ingestion from GitHub to ADLS Gen2 (Bronze) |
| **Azure Data Lake Gen2** | Storage for all layers: Bronze, Silver, and Gold |
| **Azure Databricks**     | Data transformation using **PySpark**            |
| **PySpark**              | Distributed processing and data transformation   |
| **Azure Synapse SQL**    | External tables for querying curated datasets    |
| **Power BI**             | Interactive reporting and dashboard creation     |


🗃️ Project Breakdown
1️⃣ Data Ingestion (Bronze Layer)
Used Azure Data Factory to copy CSV files from GitHub into the Bronze container in ADLS.

Stored files in original structure, untouched and unprocessed.

2️⃣ Data Transformation (Silver Layer)
Set up secure access between Databricks and ADLS using Managed Identity.

Loaded raw data files (e.g., Product, Customer, Sales, Returns) into Databricks.

Cleaned, filtered, and performed joins using PySpark.

Saved transformed outputs into the Silver container.

3️⃣ Data Serving (Gold Layer)
Created external tables in Azure Synapse Analytics (Serverless SQL) using OPENROWSET.

Tables serve as the Gold layer for query and reporting.

4️⃣ Reporting (Power BI)
Connected Power BI directly to Synapse SQL using DirectQuery.

Built a dashboard that displays:

Total Customers (Card)

Order Quantity Trends (Line Chart)

Revenue by Product Category (Bar/Pie Chart)

Top Performing Products (Horizontal Bar)

🎓 Key Takeaways
Practical use of Medallion Architecture within Azure.

Hands-on experience with end-to-end ETL design.

Secure integration of Azure services using Managed Identity.

Proficient use of PySpark for transformation.

Real-world simulation of a Data Engineer’s responsibilities.

📷 Project Diagram
![az-makproject-architecture](https://github.com/user-attachments/assets/d05e5bfd-46e6-4740-b470-63da1587604d)


🌟 Reflection
This project represents the complete lifecycle of a Cloud Data Engineer — from data ingestion to delivering business insights. It simulates real-world practices and makes concepts like Medallion Architecture approachable for beginners.

Perfect for showcasing hands-on skills in Azure's modern data stack.

🤝 Acknowledgements
Grateful to the data engineering community for insightful content.

Special thanks to Ansh Lamba for the detailed masterclass that helped bring this project to life.

👨‍💻 Author & Contact
Author: Mohd Ahsan Khan
🔗 LinkedIn Profile
📂 Project Repository
