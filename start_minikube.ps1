Write-Host "=== Starting Minikube Setup ===" -ForegroundColor Cyan

# 1. Start Minikube
Write-Host "`n[1/7] Starting Minikube..." -ForegroundColor Green
minikube start --memory=7866 --cpus=4

# 2. Point Docker at Minikube's daemon
Write-Host "`n[2/7] Pointing Docker to Minikube..." -ForegroundColor Green
minikube docker-env | Invoke-Expression

# 3. Build the pipeline image
Write-Host "`n[3/7] Building Docker image 'ecom-pipeline'..." -ForegroundColor Green
docker build -t ecom-pipeline:latest -t ecom-pipeline:dev-v6 -t ecom-pipeline:dev .

# 4. Mount data folder into Minikube
Write-Host "`n[4/7] Mounting ./data to /data inside Minikube (Opening in new window)..." -ForegroundColor Green
Start-Process minikube -ArgumentList "mount ./data:/data"
Start-Sleep -Seconds 3

# 5. Deploy Dask scheduler + workers
Write-Host "`n[5/7] Deploying Dask scheduler and workers..." -ForegroundColor Green
kubectl apply -f k8s/dask-scheduler.yaml
kubectl apply -f k8s/dask-worker.yaml

# Wait for Dask to be ready before starting the job (good practice)
Write-Host "Waiting for Dask Scheduler to be ready..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod -l app=dask-scheduler --timeout=120s

# 6. Run the pipeline job
Write-Host "`n[6/7] Applying pipeline job..." -ForegroundColor Green
kubectl apply -f k8s/pipeline-job.yaml

# 7. Port forward the dashboard and scheduler
Write-Host "`n[7/7] Port-forwarding Dask dashboard (8787) and scheduler (8786) (Opening in new windows)..." -ForegroundColor Green
Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward svc/dask-scheduler 8787:8787"
Start-Process kubectl -WindowStyle Hidden -ArgumentList "port-forward svc/dask-scheduler 8786:8786"

Write-Host "`n=== Setup Complete ===" -ForegroundColor Cyan
Write-Host "Useful commands:"
Write-Host "  Watch pods:      kubectl get pods -w"
Write-Host "  Stream logs:     kubectl logs -f job/ecom-pipeline-job"
Write-Host "  Dashboard URL:   http://localhost:8787"
Write-Host "  Scheduler URL:   tcp://localhost:8786"
