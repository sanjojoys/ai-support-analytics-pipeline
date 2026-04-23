# AI Support Analytics Pipeline

[![CI](https://github.com/sanjojoys/ai-support-analytics-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/sanjojoys/ai-support-analytics-pipeline/actions)

## Overview

This project demonstrates a fully modular, production-grade Modern Data Stack (MDS) pipeline for transforming raw SaaS product and support data into actionable business intelligence.  
It marks my transition from Data Analysis to **Analytics Engineering**, focusing on scalable, code-driven architecture with Airflow orchestration and dbt for data transformations.

- **Source data:** Synthetic but realistic SaaS product and support datasets (accounts, events, conversations, users)
- **Tools:** Apache Airflow, dbt, Python, Snowflake (or other warehouse adaptable), Docker
- **Purpose:** Clean, transform, and model operational and support data for fast analytics, KPI tracking & dashboarding.

---

## Key Features

- **Robust Airflow DAGs** orchestrate ingestion, quality checks, and multi-step transformation pipelines.
- **dbt project:** Fully modularized with staging, intermediate, and analytics marts, plus data quality tests.
- **Substantial sample data:** 500 accounts, 11,110 users, 508,576 product events, 24,311 support conversations.
- **CI/CD:** Linting & build checks with GitHub Actions.
- **Portfolio artifacts:** Markdown docs, screenshots, and dashboard links for rapid comprehension by reviewers.

---

## Why I built this

This project was built to demonstrate analytics engineering skills for product and data roles, with a focus on:

- warehouse-first data modeling
- Airflow orchestration
- dbt-based transformations
- Snowflake raw-to-mart design
- data quality testing and source freshness monitoring
- product analytics for activation, retention, experimentation, and support operations

## Business questions

This pipeline is designed to answer questions such as:

- Where do users drop off between signup and activation?
- Which onboarding actions are associated with higher retention?
- How does AI feature adoption relate to support outcomes?
- Do escalation patterns increase before churn?
- Which customer segments benefit most from AI-assisted support?

## Architecture

```text
Synthetic CSV generation
        |
        v
Snowflake RAW layer
        |
        v
dbt staging models
        |
        v
dbt intermediate models
        |
        v
dbt marts (facts + dimensions + daily KPIs)
        |
        +--> dbt tests + source freshness + docs
        |
        v
Dashboard / business summary

## Project Structure

```
.
├── ai_support_analytics/         # dbt project (models, seeds, snapshots, marts)
├── dags/                        # Airflow DAGs for orchestration
├── data/seeds/                  # Additional seed CSVs for dbt, synthetic data
├── docs/                        # Markdown documentation and diagrams
├── dashboard/                   # Portfolio screenshots, BI outputs, or dashboard links
├── tests/                       # Pytest-based data/process tests
├── .github/workflows/ci.yml     # CI: lint, test, dbt checks on push/PR
├── Dockerfile, requirements.txt  # For Airflow environment build
└── README.md                    # 📄 YOU ARE HERE!
```

---

## Getting Started

1. **Clone the repo**
   ```bash
   git clone https://github.com/sanjojoys/ai-support-analytics-pipeline.git
   cd ai-support-analytics-pipeline
   ```

2. **Start Airflow locally:**
   ```bash
   astro dev start
   ```
   - Airflow UI: [http://localhost:8080](http://localhost:8080)  
   - DB: `localhost:5432/postgres` (user: `postgres`, pw: `postgres`)

3. **(Optional) Run dbt transformations**
   ```bash
   cd ai_support_analytics
   dbt seed
   dbt run
   dbt test
   ```

4. **View artifacts:**
   - Documentation: `docs/`
   - Sample dashboards: `dashboard/`

---

## Data and Analytics

| Table / File                  | Records (approx.) | Description                                     |
|-------------------------------|-------------------|-------------------------------------------------|
| `accounts.csv`, mart models   | 500               | Companies in synthetic SaaS dataset             |
| `users.csv`                   | 11,110            | End users across accounts                       |
| `product_events.csv`          | 508,576           | App/product events for all users                |
| `support_conversations.csv`   | 24,311            | Interactions between users and support          |

All data is synthetic and safely shareable.

---

### Example Analytics

- **fct_kpis_daily:** Daily KPIs at account or system level
- **fct_support_conversations:** Support efficiency, response time
- **fact_user_journey:** Event-level user journeys from sign-up to feature adoption
- **dim_accounts:** Segmentation by company attributes

More details: [ai_support_analytics/README.md](ai_support_analytics/README.md)

---

## Airflow & Orchestration

- All DAGs are tagged and set with robust retry strategies.
- Production-ready tasks (actual Snowflake load, no placeholders).
- See `dags/ai_support_full_pipeline.py` for an end-to-end DAG.

---

## Contributing

Pull requests are welcome!

---

## Dashboard & Portfolio Artifacts

- Screenshots, dashboards, and process diagrams: [dashboard/](dashboard/)
- Documentation: [docs/](docs/)

---

## License

[MIT](LICENSE)

---

## Contact

- Owner: [Sanjo Joy (@sanjojoys)](https://github.com/sanjojoys)
