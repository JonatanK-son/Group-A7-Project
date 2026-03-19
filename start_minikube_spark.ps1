Write-Host "=== Starting Minikube Setup (Spark) ===" -ForegroundColor Cyan

# 0. Pre-checks: Install Minikube if missing and check for data
Write-Host "`n[0/6] Checking prerequisites..." -ForegroundColor Green
if (!(Get-Command minikube -ErrorAction SilentlyContinue)) {
    Write-Host "Minikube not found. Attempting to install via winget..." -ForegroundColor Yellow
    winget install --id Kubernetes.minikube -e --source winget
    if (!(Get-Command minikube -ErrorAction SilentlyContinue)) {
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

# 4. Copy data folder into Minikube
Write-Host "`n[4/6] Copying ./data to /data inside Minikube..." -ForegroundColor Green
minikube ssh "sudo mkdir -p /data && sudo chmod 777 /data"
minikube cp ./data/ minikube:/data/

# 5. Deploy Spark clusters
Write-Host "`n[5/6] Deploying Spark clusters..." -ForegroundColor Green
kubectl apply -f k8s/spark-master.yaml
kubectl apply -f k8s/spark-worker.yaml

Write-Host "Restarting pods to ensure they bind the fresh data mount..." -ForegroundColor Yellow
kubectl rollout restart deployment/spark-master
kubectl rollout restart deployment/spark-worker

Write-Host "Waiting for Spark master to be Ready..." -ForegroundColor Yellow
kubectl rollout status deployment/spark-master --timeout=120s
Write-Host "Waiting for Spark workers to be Ready..." -ForegroundColor Yellow
kubectl rollout status deployment/spark-worker --timeout=120s


# 6. Port forward dashboards and schedulers
Write-Host "`n[6/6] Port-forwarding dashboards and schedulers (Opening in new windows)..." -ForegroundColor Green
Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward svc/spark-master 8080:8080"

Write-Host "`n=== Setup Complete ===" -ForegroundColor Cyan
Write-Host "Useful commands:"
Write-Host "  Watch pods:      kubectl get pods -w"
Write-Host "  Spark Dashboard: http://localhost:8080"
Write-Host "  Spark Master:    spark://localhost:7077"
