#!/bin/bash
# Script to run the Dask pipeline with dedicated cluster resources
echo -e "\033[0;36m=== Preparing Dask Benchmark ===\033[0m"

# 1. Scale down Spark (to free up memory)
kubectl delete job spark-pipeline-job --ignore-not-found 2>/dev/null

# 2. Scale up Dask workers (restore to 2 replicas)
echo -e "\033[0;32mScaling up Dask workers to 2 replicas...\033[0m"
kubectl scale deployment dask-worker --replicas=2
kubectl rollout status deployment dask-worker

# 3. Running the pipeline
echo -e "\033[0;32mStarting Dask pipeline...\033[0m"
python3 ./pipeline/run_pipeline.py

echo -e "\033[0;36m\nDask run completed.\033[0m"
