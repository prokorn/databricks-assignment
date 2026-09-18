# LAB 8 — CI/CD: DEV → PROD Promotion

Production-grade, automated promotion from DEV to PROD using **Declarative Automation Bundles (DABs)**. The entire movies analytics project — pipelines, jobs, notebooks, DDL, and unit tests — is expressed as code, version-controlled in Git, and deployed through a CI/CD pipeline with zero manual steps in PROD.

---

## Project Structure

```
Lab 8/
├── databricks.yml                    # Bundle root: variables, targets (dev/prod)
├── resources/
│   ├── movies_pipeline.yml           # DLT pipeline resource (Bronze → Silver + Quarantine)
│   └── movies_workflow.yml           # Multi-task job: Pipeline → Gold → DDL → Reconciliation → DQ
├── src/
│   ├── 01_movies_dlt_pipeline.py     # DLT pipeline source (Auto Loader, DQ rules, quarantine)
│   ├── 02_delta_constraints/         # Notebook: idempotent Delta table constraints
│   ├── 03_reconciliation.py           # Notebook: silent data-loss reconciliation audit
│   ├── 04_gold_dq_checks/             # Notebook: gold layer uniqueness & null checks
│   ├── movies_gold_layer/             # Notebook: star schema (dim_movies, dim_genres, fact, summary)
│   └── transforms.py                 # Pure Python functions (tested, imported by DLT pipeline)
├── tests/
│   └── test_transforms.py             # Pytest unit tests for transforms.py
└── README.md
```

---

## Workflow DAG

```
run_silver_pipeline (DLT pipeline_task)
  └→ build_gold_layer (notebook_task)
       └→ apply_delta_constraints (notebook_task)
            └→ reconciliation_check (notebook_task)
                 └→ gold_dq_checks (notebook_task)
```

Each notebook task receives `catalog` and `schema` as `base_parameters` (widgets), ensuring full environment decoupling.

---

## DAB Configuration

| Variable | Dev | Prod | Purpose |
| --- | --- | --- | --- |
| `catalog` | `main` | `main` | Unity Catalog |
| `schema` | `lab_data_dev` | `lab_data_prod` | Schema isolation per environment |
| `volume_path` | `/Volumes/main/lab_data/movies_volume` | `/Volumes/main/lab_data_prod/movies_volume` | Source data volume |

All components receive their target catalog/schema at runtime — **zero hardcoding** via `spark.conf.get()`, `dbutils.widgets`, and `base_parameters`.

---

## CI/CD Pipeline

### On Pull Request
1. Run unit tests (`pytest tests/`)
2. Validate bundle (`databricks bundle validate --strict --target dev`)
3. Block merge on any failure

### On Merge to `main`
1. Deploy to PROD (`databricks bundle deploy --target prod --auto-approve`)
2. Run the production job (`databricks bundle run movies_daily_dq_workflow --target prod`)

### GitHub Actions Example

```yaml
name: Deploy to PROD
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/cli/main/install.sh | sh
      - name: Configure CLI
        run: |
          echo "[DEFAULT]\nhost = ${{ secrets.DATABRICKS_HOST }}\ntoken = ${{ secrets.DATABRICKS_TOKEN }}" > ~/.databrickscfg
      - name: Run tests
        run: pytest tests/ -v
      - name: Validate bundle
        run: databricks bundle validate --strict --target dev

  deploy:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/cli/main/install.sh | sh
      - name: Configure CLI (PROD)
        run: |
          echo "[DEFAULT]\nhost = ${{ secrets.PROD_HOST }}\ntoken = ${{ secrets.PROD_TOKEN }}" > ~/.databrickscfg
      - name: Deploy to PROD
        run: databricks bundle deploy --target prod --auto-approve
      - name: Run production job
        run: databricks bundle run movies_daily_dq_workflow --target prod
```

---

## Deploy & Run

```bash
# DEV
 databricks bundle validate --strict --target dev
 databricks bundle deploy --target dev
 databricks bundle run movies_daily_dq_workflow --target dev

# PROD
 databricks bundle validate --strict --target prod
 databricks bundle deploy --target prod --auto-approve
 databricks bundle run movies_daily_dq_workflow --target prod
```