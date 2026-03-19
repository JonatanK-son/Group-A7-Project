#!/bin/bash

# Colors for output
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${CYAN}=== Starting Minikube Setup (Dask) ===${NC}"

echo -e "\n${GREEN}[0/6] Checking prerequisites...${NC}"
if ! command -v minikube &> /dev/null || ! command -v kubectl &> /dev/null; then
    echo -e "${RED}Minikube or kubectl not found. Please install them.${NC}"
    exit 1
fi
if [ ! -d "./data" ] || [ -z "$(find ./data -name '*.csv' 2>/dev/null)" ]; then
    echo -e "${YELLOW}Data not found. Downloading sample data...${NC}"
    uv run python scripts/download_data.py --sample
fi

echo -e "\n${GREEN}[1/6] Starting Minikube...${NC}"
minikube start --memory=7866 --cpus=4

echo -e "\n${GREEN}[2/6] Pointing Docker to Minikube...${NC}"
eval $(minikube docker-env)

echo -e "\n${GREEN}[3/6] Building Docker image 'ecom-pipeline'...${NC}"
docker build -t ecom-pipeline:latest -t ecom-pipeline:dev-v6 -t ecom-pipeline:dev .

echo -e "\n${GREEN}[4/6] Copying ./data to /data inside Minikube...${NC}"
minikube ssh "sudo mkdir -p /data && sudo chmod 777 /data"
minikube cp ./data/ minikube:/data/

echo -e "\n${GREEN}[5/6] Deploying Dask cluster...${NC}"
kubectl apply -f k8s/dask-scheduler.yaml
kubectl apply -f k8s/dask-worker.yaml

echo -e "${YELLOW}Restarting pods to ensure they bind the fresh data mount...${NC}"
kubectl rollout restart deployment/dask-scheduler
kubectl rollout restart deployment/dask-worker

echo -e "${YELLOW}Waiting for Dask scheduler to be Ready...${NC}"
kubectl rollout status deployment/dask-scheduler --timeout=120s
echo -e "${YELLOW}Waiting for Dask workers to be Ready...${NC}"
kubectl rollout status deployment/dask-worker --timeout=120s

echo -e "\n${GREEN}[6/6] Port-forwarding dashboard and scheduler...${NC}"
kubectl port-forward svc/dask-scheduler 8787:8787 > /dev/null 2>&1 &
kubectl port-forward svc/dask-scheduler 8786:8786 > /dev/null 2>&1 &

echo -e "\n${CYAN}=== Setup Complete ===${NC}"
echo "Useful commands:"
echo "  Watch pods:      kubectl get pods -w"
echo "  Dask Dashboard:  http://localhost:8787"
echo "  Dask Scheduler:  tcp://localhost:8786"
