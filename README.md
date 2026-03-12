# E-Commerce Behaviour Pipeline — Group A7

Multi-stage distributed data pipeline processing **~14.6 GB** of e-commerce event data using **Dask** and **Apache Spark**, deployable to a local Minikube cluster.

---

## Dataset

Download from [Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) and place the extracted files in `data/`:

```
data/
├── 2019-Oct.csv   (5.6 GB)
└── 2019-Nov.csv   (9.0 GB)
```

---

## Project Structure

```
.
├── data/                        # raw CSV files (not committed)
├── output/
│   ├── parquet/validated/       # cleaned intermediate Parquet (partitioned by event_type)
│   └── results/                 # final analysis Parquet files
├── src/
│   ├── config.py                # all paths and schema constants
│   ├── logger.py                # structured JSON logger + timer
│   ├── validation.py            # schema enforcement, null/range checks, cleaning
│   ├── ingestion.py             # CSV loader with retry + merge utilities
│   ├── transformations_dask.py  # 5 Dask analyses
│   ├── transformations_spark.py # 4 Spark analyses (incl. window function)
│   └── storage.py               # Parquet read/write helpers
├── pipeline/
│   └── run_pipeline.py          # standalone runner (used by K8s Job)
├── notebooks/
│   └── pipeline.ipynb           # interactive end-to-end notebook
├── k8s/
│   ├── dask-scheduler.yaml      # Dask scheduler Deployment + Services
│   ├── dask-worker.yaml         # Dask worker Deployment (3 replicas)
│   └── pipeline-job.yaml        # Kubernetes Job
├── Dockerfile
└── requirements.txt
```

---

## Quick Start (local)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Open the notebook
jupyter notebook notebooks/pipeline.ipynb

# 3. Or run the standalone script
python pipeline/run_pipeline.py
```

---

## Storage Strategy

| Layer | Format | Location | Rationale |
|---|---|---|---|
| Raw input | CSV | `data/` | Unchanged source of truth |
| Validated intermediate | **Parquet** partitioned by `event_type` | `output/parquet/validated/` | Predicate pushdown filters whole partitions at read time |
| Analysis results | **Parquet** (single file each) | `output/results/` | Compact, typed, BI-tool ready |

---

## Kubernetes Deployment (Minikube)

### Prerequisites
- [Minikube](https://minikube.sigs.k8s.io/) installed and running
- [kubectl](https://kubernetes.io/docs/tasks/tools/) configured

### Steps

```bash
# 1. Start Minikube
minikube start --memory=8192 --cpus=4

# 2. Point Docker at Minikube's daemon (builds image directly into cluster)
eval $(minikube docker-env)          # Linux/macOS
# minikube docker-env | Invoke-Expression   # PowerShell

# 3. Build the pipeline image
docker build -t ecom-pipeline:latest .

# 4. Mount data folder into Minikube (keep this terminal open)
minikube mount ./data:/data

# 5. Deploy Dask scheduler + workers
kubectl apply -f k8s/dask-scheduler.yaml
kubectl apply -f k8s/dask-worker.yaml

# 6. Wait for pods to be Ready
kubectl get pods -w

# 7. Run the pipeline Job
kubectl apply -f k8s/pipeline-job.yaml

# 8. Stream logs
kubectl logs -f job/ecom-pipeline-job

# 9. Access Dask dashboard (opens browser)
minikube service dask-dashboard
```

### Scale workers

```bash
kubectl scale deployment dask-worker --replicas=5
```

---

## Pipeline Stages & Operations

| Stage | Framework | Operations |
|---|---|---|
| Ingestion | Dask | multi-file concat, CSV parse, retry logic |
| Validation | — | schema check, null check, value-range assertion, row cleaning |
| Revenue by category | Dask + Spark | **filter** + **groupBy aggregate** |
| Conversion funnel | Dask + Spark | **filter** + **pivot aggregate** |
| Hourly activity | Dask + Spark | **assign** + **groupBy** |
| Session statistics | Dask | advanced **groupBy.agg** (min/max/count/sum) |
| Top brands | Dask | **filter** + **nlargest** |
| Product window rank | Spark | **RANK() OVER PARTITION BY** (window function) |
| Storage | — | write/read **Parquet** |

---

## Quality & Operations Features

- **Structured logging** (`src/logger.py`) — every event is JSON with timestamp, level, and key/value context
- **Retry logic** (`src/ingestion.py`) — transient I/O failures retry up to 3 times
- **Schema enforcement** (`src/validation.py`) — raises `RuntimeError` on missing columns
- **Null checks & range assertions** — drops rows with null keys and negative prices
- **Edge-case handling** — `on_bad_lines='skip'` silently drops malformed CSV rows; empty-string `user_session` filtered out
- **Execution monitoring** — per-stage timing captured in `timings` dict and printed at end
