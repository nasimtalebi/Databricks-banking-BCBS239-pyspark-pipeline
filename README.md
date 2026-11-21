## Databricks-banking-Pyspark-pipeline-Reconciliation، Plausibility و Regression
### Databricks Banking PySpark Pipeline (BCBS 239 & Data Quality)

An end-to-end banking data modeling, behavioral window analytics, and automated Data Quality (DQ) pipeline built with **PySpark** and **Delta Lake** in **Databricks**, designed with **BCBS 239** risk data aggregation and governance principles in mind.

## Architecture & Delta Tables

The pipeline ingests raw banking source files from `/Volumes/workspace/default/raw_banking` and produces curated dimensional and fact Delta tables in Unity Catalog (`workspace.default`):

- `workspace.default.dim_accounts` — Account dimension table (`1,651` records).
- `workspace.default.dim_customers` — Customer dimension table (`1,100` records).
- `workspace.default.fact_transactions_enriched` — Enriched transaction fact table (`50,000` rows, `49,500` distinct `TransactionID`s, total amount `125,194,599.50`) with account-level window analytics (`txn_seq_num`, `running_total_amount`, etc.).
- `workspace.default.dq_audit_log` — Append-only Delta audit log capturing UTC execution timestamps, target table name, gate status (`PASSED_WITH_WARN`), row/ID/amount metrics, and warning counts for lineage and auditability.

## Data Quality, Regression & BCBS 239 Governance

Automated checks (`run_banking_dq_suite`) validate every run across four layers:

1. **Reconciliation:** Verifies exact alignment between raw transactions and `fact_transactions_enriched` across row count (`50,000` vs. `50,000`, diff `0`), distinct `TransactionID` count (`49,500`), and total `Amount` (`125,194,599.50`, diff `0.000000`).
2. **Critical Plausibility & Referential Integrity:** Enforces `0` failures for non-positive/null `Amount`, null `TransactionID` or `AccountOriginID`, orphaned `AccountOriginID` keys against `dim_accounts`, missing `CustomerID` links, and future-dated transactions.
3. **Warning-Level Audit Handling:** Identifies `1,000` transactions with a `NULL` `TransactionDate`. Rather than silently deleting records and breaking financial totals or audit trails, these rows are preserved in the fact table, ordered safely in window logic, flagged as a warning (`PASSED_WITH_WARN`), and recorded in `dq_audit_log`.
4. **Regression Baseline Comparison:** Compares current row counts, distinct transaction IDs, and total amounts against the recorded baseline snapshot (`df_regression_baseline`) within configurable tolerance (`amount_tolerance=0.01`).

## Session Resilience & Execution

- Cells include session-safe fallbacks to persisted Delta tables (`workspace.default.*`) and source files so checks can run reliably even after a session reset or `NameError`.
- Critical gate failures raise a `ValueError` when `strict_mode=True` to prevent unverified data from progressing downstream.
- Designed to support scheduled execution via **Databricks Workflows / Schedule** alongside version-controlled commits and Pull Requests on GitHub.
