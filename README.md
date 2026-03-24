# E-Commerce Behaviour Pipeline — Group A7

Multi-stage distributed data pipeline processing **~14.6 GB** of e-commerce event data using **Dask** and **Apache Spark**, deployable to a local Minikube cluster.

---

## Dataset
 
We use the [eCommerce behavior data from multi-category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).
 
### Easy Download
 
You can download a **~100MB sample** for local development using our utility:
 
```bash
uv run python scripts/download_data.py --sample
```
 
For the full dataset:
 
```bash
uv run python scripts/download_data.py
```
 
For more details, see [docs/data.md](docs/data.md).

---

## Project Structure

```
.
├── data/                        # raw CSV files (not committed)
├── docs/                        # project documentation
├── output/
│   ├── parquet/validated/       # cleaned intermediate Parquet (partitioned by event_type)
│   └── results/                 # final analysis Parquet files
├── scripts/
│   └── download_data.py         # data download utility
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
├── pyproject.toml               # project metadata and dependencies
├── uv.lock                      # lockfile for reproducible environments
```

---

### Quick Start (Local)

If you have [just](https://github.com/casey/just) installed:

```bash
just sync           # Install dependencies
just data-sample    # Download ~100MB sample
just pipeline       # Run Dask pipeline locally
just save-figures   # Generate charts from results
```

Without `just`:

```bash
uv sync
uv run python scripts/download_data.py --sample
uv run python pipeline/run_pipeline.py
uv run python scripts/save_figures.py
```

---

## Distributed Execution (Minikube)

The pipeline is fully containerized and can be deployed to a local Minikube cluster using either **Dask** or **Apache Spark** as the engine.

### 1. Cluster Setup (All Platforms)

The project includes an automated setup script that handles:
- Starting **Minikube** with optimized resources.
- Pointing your **Docker** daemon to the cluster.
- **Building** the pipeline image directly into the cluster.
- **Mounting** your local data folders.
- **Deploying** the Dask scheduler, workers, and Spark RBAC.
- Establishing **Port-Forwards** for the dashboards.

#### **Run the setup script for your platform:**

*   **Linux (Bash)**:
    ```bash
    bash scripts/start_minikube.sh
    ```
*   **Windows (PowerShell)**:
    ```powershell
    .\scripts\start_minikube.ps1
    ```

---

### 2. Running on Linux (Bash)

The project includes shell scripts in the `scripts/` directory for automated deployment.

*   **Start Dask**:
    ```bash
    bash scripts/run_dask.sh        # Scales Dask and runs the job
    ```
*   **Start Spark**:
    ```bash
    bash scripts/run_spark.sh       # Scales down Dask and runs Spark Job
    ```

### 3. Running on Windows (PowerShell)

Use the curated `.ps1` scripts for a seamless Windows experience.

*   **Start Dask**:
    ```powershell
    .\scripts\run_dask.ps1          # Scales Dask and runs the job
    ```
*   **Start Spark**:
    ```powershell
    .\scripts\run_spark.ps1         # Scales down Dask and runs Spark Job
    ```

---

## Monitoring & Dashboards

Both frameworks provide real-time dashboards for monitoring task execution and memory usage.

*   **Dask Dashboard**:
    Once the scheduler pod is running, access it at:
    `minikube service dask-dashboard` (typically `http://localhost:8787`)
*   **Spark UI**:
    The `run_spark` scripts automatically establish a port-forward to:
    `http://localhost:4040` (while the job is active)

---

## Results & Visualizations

After the pipeline completes, results are stored in `output/results/` (Dask) or `output/results_spark/` (Spark). To generate visual charts:

```bash
uv run python scripts/save_figures.py
```
Outputs are saved in `output/figures/` as high-resolution PNGs.

---

## Infrastructure Scaling (Dask)

You can dynamically adjust the number of Dask workers to match your cluster resources:

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

## Scaling to the Cloud

For production workloads exceeding the capacity of a local Minikube cluster, the pipeline can be transitioned to cloud-native Kubernetes (GKE, EKS, or AKS) following these steps:

### 1. Cloud Storage Migration
The current `hostPath` mounts in `k8s/*.yaml` should be replaced with cloud-native object storage:
*   **Infrastructure**: Upload raw CSVs to **GCS** (Google Cloud Storage) or **Amazon S3**.
*   **Code Update**: Update `src/config.py` to use `s3://` or `gs://` prefixes. Dask and Spark use `fsspec` and `hadoop-aws`/`gcsfs` to read these natively.
*   **Performance**: Cloud storage provides high-throughput concurrent reads, allowing the cluster to scale to hundreds of workers without I/O bottlenecks.

### 2. Deployment to Managed Kubernetes
*   **Build & Push**: Build the Docker image and push it to a private container registry (GCR, ECR).
*   **Volume Configuration**: Replace `hostPath` volumes in `dask-worker.yaml` and `spark-job.yaml` with **Persistent Volume Claims (PVCs)** or direct cloud storage connectors.
*   **Resource Allocation**: Adjust `resources` requests and limits in the YAML manifests to utilize high-memory machine types (e.g., `n2-highmem-8`).

### 3. Auto-scaling & Spot Instances
*   **Vertical Scaling**: Use larger `split_out` values in Dask to handle multi-terabyte datasets.
*   **Cost Optimization**: Deploy Dask workers on **Spot/Preemptible instances**. Dask's resilience handles worker terminations gracefully by re-computing lost partitions.
*   **Cluster Auto-scaler**: Enable Kubernetes auto-scaling to dynamically provision nodes based on the Dask task queue length.

---

---

## Quality & Operations Features

- **Structured logging** (`src/logger.py`) — every event is JSON with timestamp, level, and key/value context
- **Retry logic** (`src/ingestion.py`) — transient I/O failures retry up to 3 times
- **Schema enforcement** (`src/validation.py`) — raises `RuntimeError` on missing columns
- **Null checks & range assertions** — drops rows with null keys and negative prices
- **Edge-case handling** — `on_bad_lines='skip'` silently drops malformed CSV rows; empty-string `user_session` filtered out
- **Execution monitoring** — per-stage timing captured in `timings` dict and printed at end
