# LAB 7 — Data Quality Testing & Unit Tests

## Overview
This project establishes testing frameworks across code logic and data integrity within the Medallion architecture, ensuring reliability before CI/CD automation.

## Part A: Unit Testing
* **Module:** `transforms.py` encapsulates pure PySpark transformations (`clean_silver_metadata`, `add_surrogate_movie_key`, `split_and_trim_genres`).
* **Test Suite:** `test_transforms.py` executes targeted unit tests via `pytest` to assert schema types, MD5 surrogate determinism, and string parsing.

## Part B: Data Quality Gates (DQX)
1. **In-Pipeline Expectations & Quarantine Pattern:**
   * Pipeline: `01_movies_dlt_pipeline.py`.
   * Enforces rules for Completeness (`valid_title`) and Validity ranges (`valid_release_year`, `valid_rating`).
   * Invalid records are rerouted to `movies_quarantine` with categorized `failure_reason` attributes rather than being dropped silently.
2. **Delta Constraints (Storage Layer):**
   * Script: Applied via SQL on `dim_movies` and `dim_genres`.
   * `CHECK (movie_id IS NOT NULL)` and `CHECK (length(title) > 0)` enforce table-level integrity on write operations.
3. **Reconciliation Audit:**
   * Script: `reconciliation.py`.
   * Verifies conservation of records across stages:
     $$\text{Bronze Count (21069)} = \text{Silver Count (916)} + \text{Quarantine Count (20153)}$$
   * Result: **0 silent data loss**.