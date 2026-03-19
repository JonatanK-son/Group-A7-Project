Write-Host "=== Starting Minikube Setup ===" -ForegroundColor Cyan

# 0. Pre-checks: Install Minikube if missing and check for data
Write-Host "`n[0/6] Checking prerequisites..." -ForegroundColor Green
if (!(Get-Command minikube -ErrorAction SilentlyContinue)) {
    Write-Host "Minikube not found. Attempting to install via winget..." -ForegroundColor Yellow
    winget install --id Kubernetes.minikube -e --source winget
    if (!(Get-Command minikube -ErrorAction SilentlyContinue)) {
        # Refresh path for the current session
        $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
        if (!(Get-Command minikube -ErrorAction SilentlyContinue)) {
            Write-Error "Minikube installation failed or requires a terminal restart."
            exit 1
        }
    }
}

if (!(Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Write-Host "kubectl not found. Attempting to install via winget..." -ForegroundColor Yellow
    winget install --id Kubernetes.kubectl -e --source winget
    $env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path", "User")
}

if (!(Test-Path "./data") -or !(Get-ChildItem "./data/*.csv" -ErrorAction SilentlyContinue)) {
    Write-Host "Data not found. Downloading sample data..." -ForegroundColor Yellow
    uv run python scripts/download_data.py --sample
}

# 1. Start Minikube
Write-Host "`n[1/6] Starting Minikube..." -ForegroundColor Green
minikube start --memory=7866 --cpus=4

# 2. Point Docker at Minikube's daemon
Write-Host "`n[2/6] Pointing Docker to Minikube..." -ForegroundColor Green
minikube docker-env | Invoke-Expression

# 3. Build the pipeline image
Write-Host "`n[3/6] Building Docker image 'ecom-pipeline'..." -ForegroundColor Green
docker build -t ecom-pipeline:latest -t ecom-pipeline:dev-v6 -t ecom-pipeline:dev .

# 4. Mount data folder into Minikube
Write-Host "`n[4/6] Mounting ./data to /data inside Minikube (Opening in new window)..." -ForegroundColor Green
Start-Process minikube -ArgumentList "mount ./data:/data"

# Wait until the CSV files are actually visible inside Minikube before deploying.
# minikube mount takes a few seconds to establish the 9p-fs link; polling is
# more reliable than a fixed sleep on slow machines.
Write-Host "Waiting for mount to become ready inside Minikube..." -ForegroundColor Yellow
$MountTimeout = 60
$MountElapsed = 0
$MountReady = $false
while ($MountElapsed -lt $MountTimeout) {
    $check = minikube ssh "ls /data/*.csv 2>/dev/null | head -1" 2>$null
    if ($check) {
        $MountReady = $true
        Write-Host "  Mount confirmed: CSV files visible at /data inside Minikube." -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 2
    $MountElapsed += 2
}
if (-not $MountReady) {
    Write-Error "Timed out waiting for minikube mount. Check that 'minikube mount ./data:/data' is still running."
    exit 1
}

# 5. Deploy Dask & Spark clusters (or restart them if already running so they
#    pick up the now-populated hostPath volume with a fresh pod start).
Write-Host "`n[5/6] Deploying Dask and Spark clusters..." -ForegroundColor Green
kubectl apply -f k8s/dask-scheduler.yaml
kubectl apply -f k8s/dask-worker.yaml
kubectl apply -f k8s/spark-master.yaml
kubectl apply -f k8s/spark-worker.yaml

# Force pods to restart so they re-bind the hostPath volume from scratch.
# This is a no-op for brand-new deployments and fixes the race for re-runs.
Write-Host "Restarting pods to ensure they bind the fresh data mount..." -ForegroundColor Yellow
kubectl rollout restart deployment/dask-scheduler
kubectl rollout restart deployment/dask-worker
kubectl rollout restart deployment/spark-master
kubectl rollout restart deployment/spark-worker

# Wait for all deployments to finish rolling out before port-forwarding.
# port-forward silently dies if the target pod isn't Ready yet.
Write-Host "Waiting for Dask scheduler to be Ready..." -ForegroundColor Yellow
kubectl rollout status deployment/dask-scheduler --timeout=120s
Write-Host "Waiting for Dask workers to be Ready..." -ForegroundColor Yellow
kubectl rollout status deployment/dask-worker --timeout=120s
Write-Host "Waiting for Spark master to be Ready..." -ForegroundColor Yellow
kubectl rollout status deployment/spark-master --timeout=120s
Write-Host "Waiting for Spark workers to be Ready..." -ForegroundColor Yellow
kubectl rollout status deployment/spark-worker --timeout=120s

# 6. Port forward dashboards and schedulers
Write-Host "`n[6/6] Port-forwarding dashboards and schedulers (Opening in new windows)..." -ForegroundColor Green
Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward svc/dask-scheduler 8787:8787"
Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward svc/dask-scheduler 8786:8786"
Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward svc/spark-master 8080:8080"

Write-Host "`n=== Setup Complete ===" -ForegroundColor Cyan
Write-Host "Useful commands:"
Write-Host "  Watch pods:      kubectl get pods -w"
Write-Host "  Stream logs:     kubectl logs -f job/ecom-pipeline-job"
Write-Host "  Dask Dashboard:  http://localhost:8787"
Write-Host "  Dask Scheduler:  tcp://localhost:8786"
Write-Host "  Spark Dashboard: http://localhost:8080"
Write-Host "  Spark Master:    spark://localhost:7077"
