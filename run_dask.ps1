# Powershell script to run the Dask pipeline with dedicated cluster resources
Write-Host "=== Preparing Dask Benchmark ===" -ForegroundColor Cyan

# 1. Scale down Spark (to free up memory)
kubectl delete job spark-pipeline-job --ignore-not-found 2>$null

# 2. Scale up Dask workers (restore to 2 replicas)
Write-Host "Scaling up Dask workers to 2 replicas..." -ForegroundColor Green
kubectl scale deployment dask-worker --replicas=2
kubectl rollout status deployment dask-worker

# 3. Running the pipeline
Write-Host "Starting Dask pipeline..." -ForegroundColor Green
uv run python ./pipeline/run_pipeline.py

Write-Host "`nDask run completed." -ForegroundColor Cyan
