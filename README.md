# NYC Taxi Trips Data Warehouse Project 🚖

This project demonstrates building a modern data warehouse using **AWS S3**, **Snowflake**, and **dbt** (Data Build Tool) to model, transform, and analyze NYC taxi trip data.

---

## 📁 Project Overview

- 🔹 **Dataset:** NYC Taxi Trips  
- 🔹 **Cloud Storage:** AWS S3 (used to store raw CSV datasets)  
- 🔹 **Data Warehouse:** Snowflake (cloud-native DWH)  
- 🔹 **Transformation Tool:** dbt (modular SQL modeling)  
- 🔹 **Pipeline Type:** ELT with support for **incremental loading**  
- 🔹 **Data Layers:**
  - **Raw Layer:** Direct load from S3 using Snowflake external stage
  - **Staging Layer:** Cleaned and standardized schemas
  - **Analytics Layer:** Fact and Dimension tables for business use cases

---

## 🔁 Incremental Loading

Some tables (such as `fact_trips`) are designed for **incremental loading** using `is_incremental()` in dbt. This ensures efficiency in data refresh and scalability.

---

## 🧪 Testing & Documentation

### ✅ Generic Tests
- `not_null`, `unique`, and `relationships` constraints on critical fields

### 🧠 Singular Tests (Custom Logic)
Custom SQL-based tests written in the `tests/` folder, including:
- Trips with unrealistic distances (`distance_check.sql`)
- Fare amount validation (`fare_amt_check.sql`)
- Passenger count sanity checks (`passenger_count_check.sql`)

### 📄 Auto-generated Docs
Full documentation generated using `dbt docs generate` and `dbt docs serve`.

---

## 📊 KPIs & Analysis

Business questions answered using dbt models in the `analyses/` folder:
- Total revenue by day
- Peak trip hours
- Most common payment type
- Suspicious trips (e.g., very long trips, zero fare)
- Revenue per mile
- Cross-borough traffic

---

## 📐 Data Model Diagram

![Data Model Diagram](images/data_model.png)

---

## 🚀 Data Pipeline Architecture

![Data Pipeline Architecture](images/data_pipeline.png)

---

## 📁 Folder Structure

```bash
NYC_Taxi_Trips_Project/
│
├── models/
│   ├── stage/           # Cleaned staging models
│   ├── dim/             # Dimension tables
│   └── fact/            # Fact tables (fact_trips)
│
├── analyses/            # Business questions & KPI answers
├── tests/               # Singular custom tests
├── seeds/               # Optional seed data
├── snapshots/           # (Not used but structured for future SCD)
├── macros/              # Custom macros if needed
├── dbt_project.yml      # DBT configuration
├── README.md
└── images/              # Diagrams used in README
