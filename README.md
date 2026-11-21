## Databricks-banking-Pyspark-pipeline-Reconciliation، Plausibility و Regression
# Databricks Banking PySpark Pipeline (BCBS 239 & Data Quality Governance)

An end-to-end banking data modeling, behavioral window analytics, and automated Data Quality (DQ) pipeline built with **PySpark** and **Delta Lake** in **Databricks**, engineered around **BCBS 239** risk data aggregation, lineage, and governance principles.

---

## 1. Architecture & Storage Layers (Unity Catalog)

The pipeline separates raw file ingestion from governed, query-ready analytical Delta tables inside Unity Catalog (`workspace.default`):

* **Raw Ingestion Layer (`Volume`):**
  * `/Volumes/workspace/default/raw_banking` — Stores immutable raw banking source files (accounts, customers, transactions) to preserve source-to-target lineage.
* **Curated Analytical & Governance Layer (`Delta Tables`):**
  * `workspace.default.dim_accounts` — Account dimension table (`1,651` records).
  * `workspace.default.dim_customers` — Customer dimension table (`1,100` records).
  * `workspace.default.fact_transactions_enriched` — Enriched transaction fact table (`50,000` rows, `49,500` distinct `TransactionID`s, total amount `125,194,599.50`) enriched with account-level PySpark window analytics.
  * `workspace.default.dq_audit_log` — Append-only Delta audit log storing UTC execution timestamps, target table metadata, quality gate status (`PASSED_WITH_WARN`), row/ID/amount metrics, and warning counts.

---

## 2. PySpark Data Modeling & Transformations

* **Star-Schema Dimensional Modeling:** Joins raw transaction events with account and customer dimensions using left-join enrichment so no source transactions are silently dropped prior to quality validation.
* **Account-Level Window Analytics (`pyspark.sql.window`):** Computes partitioned behavioral metrics (such as transaction sequence numbers and cumulative running amounts per account) while preserving transaction-level granularity.
* **Session-Resilient Execution:** DataFrames include deterministic fallbacks to persisted Delta tables (`workspace.default.*`) and raw files in the Unity Catalog Volume so cells run cleanly across fresh compute sessions or scheduled jobs.

---

## 3. Automated Data Quality & BCBS 239 Controls (`run_banking_dq_suite`)

Every execution validates the pipeline across four automated governance layers:

1. **Source-to-Target Reconciliation (Completeness & Accuracy):**
   * Reconciles raw transactions against `fact_transactions_enriched` across row count (`50,000` vs. `50,000`, diff `0`), distinct `TransactionID` count (`49,500`), and total `Amount` (`125,194,599.50`, diff `0.000000`).
2. **Critical Plausibility & Referential Integrity:**
   * Enforces `0` tolerance (`strict_mode=True`) for null/non-positive `Amount`, null `TransactionID` or `AccountOriginID`, orphaned `AccountOriginID` keys against `dim_accounts`, missing `CustomerID` mappings, and future-dated transactions.
3. **Transparent Warning & Exception Governance:**
   * Identifies `1,000` transactions with `NULL` `TransactionDate`. Rather than silently deleting records—which would distort financial totals and break audit lineage—these records are preserved in the fact table, ordered safely in window logic, flagged as `PASSED_WITH_WARN`, and logged in `dq_audit_log`.
4. **Regression Baseline Comparison:**
   * Compares current row counts, distinct transaction IDs, and total amounts against the recorded baseline snapshot (`df_regression_baseline`) within configurable tolerance (`amount_tolerance=0.01`).

---

## 4. Operationalization: Databricks Jobs & Scheduling

The notebook is structured for production scheduling via **Databricks Jobs & Pipelines**:

* **Task Configuration:** Notebook task running on Serverless or dedicated job compute, executing end-to-end ingestion, dimensional modeling, window enrichment, DQ checks, Delta persistence, and audit logging.
* **Schedule & Triggering:** Configurable for daily scheduled runs or event/on-demand triggers when new files arrive in `/Volumes/workspace/default/raw_banking`.
* **Fail-Fast Governance (`strict_mode=True`):** Any critical DQ failure raises a `ValueError`, halting the job immediately so unverified data never propagates to downstream risk or regulatory reporting.
* **Auditability:** Every run appends an immutable audit record to `workspace.default.dq_audit_log` and records run telemetry in Databricks Job Run history.
