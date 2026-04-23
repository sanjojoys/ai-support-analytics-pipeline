***

# 🚀 AI Support Analytics Pipeline: A Medallion Architecture Implementation

[![CI](https://github.com/sanjojoys/ai-support-analytics-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/sanjojoys/ai-support-analytics-pipeline/actions)

## 🎯 Project Overview
This project demonstrates a production-grade Modern Data Stack (MDS) built to transform messy SaaS operational data into high-fidelity business intelligence. 

It marks my transition from **Data Analysis to Analytics Engineering**, moving beyond static reporting to building scalable, self-healing data infrastructure. By treating **Data as Code**, this pipeline ensures that product and support insights are automated, tested, and warehouse-optimized.



### 🛠 The Tech Stack
* **Orchestration:** Apache Airflow (Astro CLI)
* **Transformation:** dbt Core (Data Build Tool)
* **Warehouse:** Snowflake
* **Ingestion:** Python (Custom Synthetic Data Generators)
* **BI Layer:** Looker Studio / Tableau
* **CI/CD:** GitHub Actions (Linting & SQL Fluff)

---

## 🏗 Data Architecture & Modeling
I implemented a **Medallion Architecture** to ensure data integrity and clear lineage:

1.  **Bronze (Raw):** Immutable CSV data ingested into Snowflake via Airflow.
2.  **Silver (Staging & Intermediate):** * **Staging:** Source-aligned cleanup, type casting, and normalization (Materialized as Views).
    * **Intermediate:** Complex sessionization, deduplication, and retention logic (Materialized as Transient Tables to optimize cost).
3.  **Gold (Marts):** High-performance Star Schema (Facts & Dimensions) optimized for BI tools.



---

## 💡 Business Questions Answered
This pipeline isn't just "plumbing"; it's built to answer critical product-led growth questions:
* **Retention:** Does AI feature adoption in the first 7 days correlate with D28 retention?
* **Support ROI:** Which customer segments experience the highest resolution efficiency through AI assistance?
* **Churn Signals:** Can we identify "High Ticket / Low Engagement" accounts before they churn?

---

## 🛠 Engineering Deep Dive

### 1. Robust Orchestration (Airflow)
I utilized Airflow to manage the data lifecycle across four dedicated DAGs:
* `ingest_raw_daily`: Bulk-loading CSVs into Snowflake stages.
* `source_freshness_daily`: Ensuring our pipeline doesn't run on stale data.
* `dbt_run_daily` & `dbt_test_daily`: Modular transformation and quality gating.

### 2. Custom dbt Macros
To overcome Snowflake's default schema prefixing, I authored a custom `generate_schema_name` macro. This allows for a clean `STAGING` vs. `MART` structure, making the warehouse intuitive for end-user analysts.

### 3. Data Quality & Testing
* **Generic Tests:** Applied `unique`, `not_null`, and `relationship` tests across all primary keys.
* **Singular Tests:** Built business-rule tests to ensure `activation_date` never precedes `signup_date`.

---

## 📊 Dashboard & Visualizations
The final output is an **Executive Health Dashboard**. 
* **KPIs:** D28 Retention Rate, Total MRR by Region, and AI Adoption Trends.
* **Deep Dive:** Correlation analysis between Health Scores and Support Escalations.

> **View Dashboard Screenshots:** [Link to dashboard folder](./dashboard/)

---

## 🚀 Getting Started

### 1. Prerequisites
* Docker Desktop
* Astro CLI
* Snowflake Account

### 2. Installation & Execution
```bash
# Clone the repository
git clone https://github.com/sanjojoys/ai-support-analytics-pipeline.git
cd ai-support-analytics-pipeline

# Start the environment
astro dev start

# Run the transformations
cd ai_support_analytics
dbt seed
dbt run
dbt test
```

---

## 📈 Data Volume at a Glance
| Entity | Records | Description |
| :--- | :--- | :--- |
| **Accounts** | 500 | SaaS companies (SMB to Enterprise) |
| **Users** | 11,110 | Individual end-users |
| **Product Events** | ~508k | Feature clicks, logins, and AI interactions |
| **Support Conv.** | ~24k | Multi-channel support interactions |

---

## 👤 Contact
**Sanjo Joy**  
* [GitHub](https://github.com/sanjojoys)
