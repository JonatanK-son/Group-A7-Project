# Powershell script to run the Spark pipeline with dedicated cluster resources
Write-Host "=== Preparing Spark Benchmark ===" -ForegroundColor Cyan

# 1. Scale down Dask (to free up all cluster memory for Spark executors)
Write-Host "Scaling down Dask workers to 0..." -ForegroundColor Green
kubectl scale deployment dask-worker --replicas=0

# 2. Cleanup old Spark job
# Add --wait to ensure the cluster is clean before starting the next job
Write-Host "Cleaning up previous Spark job..." -ForegroundColor Gray
kubectl delete job spark-pipeline-job --ignore-not-found --wait

# 3. Apply Spark Job
Write-Host "Starting Spark pipeline (Kubernetes Job Mode)..." -ForegroundColor Green
kubectl apply -f k8s/spark-job.yaml

# 4. Wait for job to start and stream logs
Write-Host "Waiting for Spark driver to start..." -ForegroundColor Yellow
kubectl wait --for=condition=Ready pod -l job-name=spark-pipeline-job --timeout=60s

# Automatic Port-Forward for Spark UI (Port 4040)
# Use Get-NetTCPConnection to find and kill any process already using 4040 (e.g., dangling kubectl)
$DanglingPort = Get-NetTCPConnection -LocalPort 4040 -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique
if ($DanglingPort) {
    Write-Host "Cleaning up dangling port-forward on 4040 (PID: $DanglingPort)..." -ForegroundColor Gray
    Stop-Process -Id $DanglingPort -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1 # Give it a moment to release
}

Write-Host "Establishing port-forward to Spark UI..." -ForegroundColor Yellow
# Specify the driver pod more reliably by finding the one with the 'spark-driver' container
$Pods = kubectl get pods -l job-name=spark-pipeline-job -o json | ConvertFrom-Json
$DriverPod = $Pods.items | Where-Object { $_.spec.containers.name -contains "spark-driver" } | Sort-Object { $_.metadata.creationTimestamp } | Select-Object -Last 1
$SparkPod = $DriverPod.metadata.name

if ($SparkPod) {
    # Give the Spark UI a few seconds to initialize
    Write-Host "Spark driver found: $SparkPod. Waiting for UI port 4040 to open..." -ForegroundColor Gray
    Start-Sleep -Seconds 5
    Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward pod/$SparkPod 4040:4040"
    Write-Host "Spark Dashboard: http://localhost:4040 (Targeting Pod: $SparkPod)" -ForegroundColor Cyan
} else {
    Write-Warning "No active Spark driver pod found for port-forwarding."
}

kubectl logs -f job/spark-pipeline-job

# 5. Clean up Spark (optional, but keeps cluster clean)
Write-Host "`nSpark run completed." -ForegroundColor Cyan
