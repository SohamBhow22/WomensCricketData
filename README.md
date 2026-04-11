# 🏏 Women’s Cricket Analytics Platform

A scalable, analytics-ready data platform built using **Medallion Architecture (Bronze → Silver → Gold)** for **ball-by-ball women’s cricket data**.

This project transforms raw JSON match files into a structured, queryable dataset for **analysis, dashboards, and future ML use cases**.

---

## 🚀 Project Overview

This platform ingests **Cricsheet-style ball-by-ball JSON data** and converts it into a clean relational model using:

* **DuckDB** (analytical database)
* **Python** (data pipeline orchestration)
* **SQL** (transformations)
* **Medallion Architecture**

---

## 🧱 Architecture

### 🟤 Bronze Layer (Raw - Persisted in DuckDB)

* Stores raw JSON files as-is
* Stores `people.csv` (player master)
* Adds metadata like `match_id` and `source_file`

**Tables:**

* `BRONZE_MATCHES`
* `BRONZE_PEOPLE`

---

### ⚪ Silver Layer (Clean + Flattened)

* Flattens nested JSON (innings → overs → deliveries)
* Standardizes schema
* Prepares data for joins and transformations

**Tables:**

* `SILVER_MATCH_INFO`
* `SILVER_DELIVERIES_RAW`

---

### 🟡 Gold Layer (Analytics-ready)

* Final dimensional model (star-like schema)
* Fully enriched ball-by-ball dataset
* Optimized for dashboards and queries

**Tables:**

**Dimensions**

* `DIM_PLAYERS`
* `DIM_TEAMS`
* `DIM_VENUES`
* `DIM_EVENTS`
* `DIM_OFFICIALS`

**Facts**

* `FACT_MATCHES`
* `FACT_MATCH_PLAYERS`
* `FACT_MATCH_OFFICIALS`
* `FACT_MATCH_AWARDS`
* `FACT_INNINGS`
* `FACT_DELIVERIES`

---

## 📁 Project Structure

```
women-cricket-analytics/
│
├── data/
│   ├── bronze/
│   │   ├── matches/
│   │   └── reference/
│   │       └── people.csv
│   ├── silver/
│   └── gold/
│       └── cricket.db
│
├── src/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── pipeline/
│   └── utils/
│
├── notebooks/
├── tests/
└── README.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Install dependencies

```bash
pip install duckdb pandas pyarrow
```

---

### 2️⃣ Prepare data

* Place match JSON files in:

  ```
  data/bronze/matches/
  ```

* Place player reference file:

  ```
  data/bronze/reference/people.csv
  ```

---

### 3️⃣ Initialize DuckDB

```python
import duckdb
conn = duckdb.connect("data/gold/cricket.db")
```

---

### 4️⃣ Run Bronze + Silver Pipeline (Notebook)

Run the pipeline steps in order:

1. Create Bronze tables
2. Load JSON + CSV into Bronze
3. Create Silver tables
4. Validate outputs

---

## 🧠 Key Design Decisions

### ✅ Ball-Level Grain

* One row = one delivery
* Supports all analytics (batting, bowling, fielding)

---

### ✅ Player Identity Resolution

```
player_name (JSON)
    ↓
registry_id (JSON)
    ↓
player_id (people.csv)
```

* Prevents duplicates
* Ensures consistent player tracking across matches

---

### ✅ DuckDB-first Approach

* Fast local analytics
* No re-reading raw files
* Perfect for notebook-based workflows

---

### ✅ Hybrid Pipeline

* Python → ingestion & orchestration
* SQL → transformations

---

## 📊 What You Can Build

* Player performance dashboards
* Batter vs bowler matchups
* Team composition analysis
* Venue-based insights
* Career progression tracking
* Match impact metrics

---

## 🔮 Future Enhancements

* Gold layer implementation (in progress)
* Streamlit dashboards
* Spark-based scaling
* dbt integration
* Data validation tests
* CI/CD pipeline

---

## 🧪 Validation (Recommended Checks)

```sql
-- Validate deliveries
SELECT COUNT(*) FROM SILVER_DELIVERIES_RAW;

-- Validate matches
SELECT COUNT(*) FROM BRONZE_MATCHES;

-- Spot check
SELECT * FROM SILVER_DELIVERIES_RAW LIMIT 10;
```

---

## 💡 Inspiration

Women’s cricket data is still under-explored.
This project aims to build a **robust analytics foundation** for deeper insights and future sports intelligence systems.

---

## 👨‍💻 Author

Built as part of a personal data engineering and analytics initiative.

---

## ⭐ If you like this project

Give it a ⭐ and feel free to contribute or extend it!
