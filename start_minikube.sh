#!/bin/bash

# Colors for output
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${CYAN}=== Starting Minikube Setup ===${NC}"

# 0. Pre-checks: Install Minikube if missing and check for data
echo -e "\n${GREEN}[0/6] Checking prerequisites...${NC}"

# Check minikube
if ! command -v minikube &> /dev/null; then
    echo -e "${YELLOW}Minikube not found. Attempting to install...${NC}"
    # Try apt for Debian/Ubuntu
    if command -v apt-get &> /dev/null; then
        sudo apt-get update
        # Download and install minikube binary
        curl -LO https://github.com/kubernetes/minikube/releases/latest/download/minikube-linux-amd64
        sudo install minikube-linux-amd64 /usr/local/bin/minikube
        rm minikube-linux-amd64
    else
        echo -e "${RED}Error: apt-get not found. Please install minikube manually.${NC}"
        exit 1
    fi
    
    if ! command -v minikube &> /dev/null; then
        echo -e "${RED}Minikube installation failed.${NC}"
        exit 1
    fi
fi

# Check kubectl
if ! command -v kubectl &> /dev/null; then
    echo -e "${YELLOW}kubectl not found. Attempting to install...${NC}"
    if command -v apt-get &> /dev/null; then
        curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
        sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
        rm kubectl
    else
        echo -e "${RED}Error: apt-get not found. Please install kubectl manually.${NC}"
        exit 1
    fi
fi

# Check data
if [ ! -d "./data" ] || [ -z "$(find ./data -name '*.csv' 2>/dev/null)" ]; then
    echo -e "${YELLOW}Data not found. Downloading sample data...${NC}"
    uv run python scripts/download_data.py --sample
fi

# 1. Start Minikube
echo -e "\n${GREEN}[1/6] Starting Minikube...${NC}"
minikube start --memory=7866 --cpus=4

# 2. Point Docker at Minikube's daemon
echo -e "\n${GREEN}[2/6] Pointing Docker to Minikube...${NC}"
eval $(minikube docker-env)

# 3. Build the pipeline image
echo -e "\n${GREEN}[3/6] Building Docker image 'ecom-pipeline'...${NC}"
docker build -t ecom-pipeline:latest -t ecom-pipeline:dev-v6 -t ecom-pipeline:dev .

# 4. Mount data folder into Minikube
echo -e "\n${GREEN}[4/6] Mounting ./data to /data inside Minikube...${NC}"
minikube mount ./data:/data &
sleep 3

# 5. Deploy Dask & Spark clusters
echo -e "\n${GREEN}[5/6] Deploying Dask and Spark clusters...${NC}"
kubectl apply -f k8s/dask-scheduler.yaml
kubectl apply -f k8s/dask-worker.yaml
kubectl apply -f k8s/spark-master.yaml
kubectl apply -f k8s/spark-worker.yaml

# Wait for schedulers to be ready
echo -e "${YELLOW}Waiting for Dask and Spark to be ready...${NC}"
kubectl wait --for=condition=ready pod -l app=dask-scheduler --timeout=120s
kubectl wait --for=condition=ready pod -l app=spark-master --timeout=120s

# 6. Port forward dashboards and schedulers
echo -e "\n${GREEN}[6/6] Port-forwarding dashboards and schedulers...${NC}"
kubectl port-forward svc/dask-scheduler 8787:8787 > /dev/null 2>&1 &
kubectl port-forward svc/dask-scheduler 8786:8786 > /dev/null 2>&1 &
kubectl port-forward svc/spark-master 8080:8080 > /dev/null 2>&1 &

echo -e "\n${CYAN}=== Setup Complete ===${NC}"
echo "Useful commands:"
echo "  Watch pods:      kubectl get pods -w"
echo "  Stream logs:     kubectl logs -f job/ecom-pipeline-job"
echo "  Dask Dashboard:  http://localhost:8787"
echo "  Dask Scheduler:  tcp://localhost:8786"
echo "  Spark Dashboard: http://localhost:8080"
echo "  Spark Master:    spark://localhost:7077"
