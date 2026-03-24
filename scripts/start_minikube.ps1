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
minikube start --memory=6144 --cpus=4

# 2. Point Docker at Minikube's daemon
Write-Host "`n[2/6] Pointing Docker to Minikube..." -ForegroundColor Green
minikube docker-env | Invoke-Expression

# 3. Build the pipeline image
Write-Host "`n[3/6] Building Docker image 'ecom-pipeline'..." -ForegroundColor Green
docker build -t ecom-pipeline:latest -t ecom-pipeline:dev-v6 -t ecom-pipeline:dev .

# 4. Mount output folder into Minikube
Write-Host "`n[4/6] Mounting ./output to /output inside Minikube (Opening in new window)..." -ForegroundColor Green
if (!(Test-Path "./output")) { New-Item -Path "./output" -ItemType Directory -Force }
Start-Process minikube -ArgumentList "mount ./output:/output"

# Wait until the Parquet files are actually visible inside Minikube before deploying.
# minikube mount takes a few seconds to establish the 9p-fs link; polling is
# more reliable than a fixed sleep on slow machines.
Write-Host "Waiting for mount to become ready and Parquet files to be visible..." -ForegroundColor Yellow
$MountTimeout = 60
$MountElapsed = 0
$MountReady = $false
while ($MountElapsed -lt $MountTimeout) {
    # Check for our marker or any parquet file. This ensures Phase 1 has run.
    $check = minikube ssh "find /output/parquet/validated -name '*.parquet' 2>/dev/null | head -1" 2>$null
    if ($check) {
        $MountReady = $true
        Write-Host "  Mount confirmed: Parquet files visible at /output/parquet/validated inside Minikube." -ForegroundColor Green
        break
    }
    Start-Sleep -Seconds 2
    $MountElapsed += 2
}
if (-not $MountReady) {
    Write-Warning "Timed out waiting for Parquet files in /output. If this is your first run, please run Phase 1 of 'run_pipeline.py' locally to generate the Parquet files, then restart this script."
    # We don't exit here because the user might be about to run Phase 1 in another terminal
}

# 5. Deploy Dask scheduler + workers (or restart them if already running so they
#    pick up the now-populated hostPath volume with a fresh pod start).
Write-Host "`n[5/6] Deploying Dask scheduler and workers..." -ForegroundColor Green
kubectl apply -f k8s/dask-scheduler.yaml
kubectl apply -f k8s/dask-worker.yaml
kubectl apply -f k8s/spark-role.yaml

# Force pods to restart so they re-bind the hostPath volume from scratch.
# This is a no-op for brand-new deployments and fixes the race for re-runs.
Write-Host "Restarting pods to ensure they bind the fresh data mount..." -ForegroundColor Yellow
kubectl rollout restart deployment/dask-scheduler
kubectl rollout restart deployment/dask-worker

# Wait for both deployments to finish rolling out before port-forwarding.
# port-forward silently dies if the target pod isn't Ready yet.
Write-Host "Waiting for Dask scheduler to be Ready..." -ForegroundColor Yellow
kubectl rollout status deployment/dask-scheduler --timeout=120s
Write-Host "Waiting for Dask workers to be Ready..." -ForegroundColor Yellow
kubectl rollout status deployment/dask-worker --timeout=120s

# 6. Port forward the dashboard and scheduler
Write-Host "`n[6/6] Port-forwarding Dask dashboard (8787) and scheduler (8786) (Opening in new windows)..." -ForegroundColor Green

# Kill any existing kubectl port-forward processes first
Get-Process kubectl -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -like "*port-forward*8787:8787*" -or $_.CommandLine -like "*port-forward*8786:8786*" } | Stop-Process -Force -ErrorAction SilentlyContinue

# Verify 8787/8786 aren't held by non-kubectl processes
$occ8787 = Get-NetTCPConnection -LocalPort 8787 -ErrorAction SilentlyContinue
if ($occ8787) { Write-Warning "Port 8787 is occupied (PID: $($occ8787.OwningProcess))" }

Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward svc/dask-scheduler 8787:8787"
Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward svc/dask-scheduler 8786:8786"

# Brief wait to verify Dask dashboard
Start-Sleep -Seconds 3
if (Test-NetConnection -ComputerName 127.0.0.1 -Port 8787 -InformationLevel Quiet) {
    Write-Host "Dask Dashboard: http://localhost:8787 (Port-forwarding active)" -ForegroundColor Green
} else {
    Write-Warning "Dask Dashboard might not be ready yet. Check http://localhost:8787 in a moment."
}

Write-Host "`n=== Setup Complete ===" -ForegroundColor Cyan
Write-Host "Useful commands:"
Write-Host "  Watch pods:      kubectl get pods -w"
Write-Host "  Stream logs:     kubectl logs -f deployment/dask-worker"
Write-Host "  Dashboard URL:   http://localhost:8787"
Write-Host "  Scheduler URL:   tcp://localhost:8786"
