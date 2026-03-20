# Powershell script to run the Spark pipeline with dedicated cluster resources
Write-Host "=== Preparing Spark Benchmark ===" -ForegroundColor Cyan

# 1. Scale down Dask (to free up all cluster memory for Spark executors)
Write-Host "Scaling down Dask workers to 0..." -ForegroundColor Green
kubectl scale deployment dask-worker --replicas=0

# 2. Cleanup old Spark job
kubectl delete job spark-pipeline-job --ignore-not-found 2>$null

# 3. Apply Spark Job
Write-Host "Starting Spark pipeline (Kubernetes Job Mode)..." -ForegroundColor Green
kubectl apply -f k8s/spark-job.yaml

# 4. Wait for job to start and stream logs
Write-Host "Waiting for Spark driver to start and show results..." -ForegroundColor Yellow
kubectl wait --for=condition=Ready pod -l job-name=spark-pipeline-job --timeout=60s

# Automatic Port-Forward for Spark UI (Port 4040)
# First, try to kill any dangling port-forward on 4040 to avoid "address already in use" errors.
Get-Process kubectl -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like "*port-forward*4040:4040*" } | Stop-Process -Force -ErrorAction SilentlyContinue

Write-Host "Establishing port-forward to Spark UI..." -ForegroundColor Yellow
$SparkPod = kubectl get pods -l job-name=spark-pipeline-job --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}'
if ($SparkPod) {
    Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward pod/$SparkPod 4040:4040"
    Write-Host "Spark Dashboard: http://localhost:4040" -ForegroundColor Cyan
} else {
    Write-Warning "No running Spark pod found for port-forwarding."
}

kubectl logs -f job/spark-pipeline-job

# 5. Clean up Spark (optional, but keeps cluster clean)
Write-Host "`nSpark run completed." -ForegroundColor Cyan
