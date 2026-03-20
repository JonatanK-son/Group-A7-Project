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

# 1. Start Minikube with host mount
echo -e "\n${GREEN}[1/6] Starting Minikube with host mount...${NC}"
mkdir -p ./output
minikube start --memory=7866 --cpus=4 --mount --mount-string="$(pwd)/output:/output"

# 2. Point Docker at Minikube's daemon
echo -e "\n${GREEN}[2/6] Pointing Docker to Minikube...${NC}"
eval $(minikube docker-env)

# 3. Build the pipeline image
echo -e "\n${GREEN}[3/6] Building Docker image 'ecom-pipeline'...${NC}"
docker build -t ecom-pipeline:latest -t ecom-pipeline:dev-v6 -t ecom-pipeline:dev .

# 4. Verify mount inside Minikube
echo -e "\n${GREEN}[4/6] Verifying mount inside Minikube...${NC}"

# Wait until the 9p mount is confirmed active in the VM.
echo -e "${YELLOW}Waiting for mount to become active...${NC}"
MOUNT_TIMEOUT=30
MOUNT_ELAPSED=0
MOUNT_READY=0
while [ $MOUNT_ELAPSED -lt $MOUNT_TIMEOUT ]; do
    if minikube ssh "mount | grep -q ' /output '" 2>/dev/null; then
        MOUNT_READY=1
        echo -e "${GREEN}  Mount confirmed: /output is active inside Minikube.${NC}"
        break
    fi
    sleep 2
    MOUNT_ELAPSED=$((MOUNT_ELAPSED + 2))
done
if [ $MOUNT_READY -eq 0 ]; then
    echo -e "${RED}Error: Minikube mount failed to activate. You may need to run 'minikube delete' and try again.${NC}"
    exit 1
fi

# Check for Parquet files — warn but don't exit (Phase 1 ingestion may run separately).
if ! minikube ssh "find /output/parquet/validated -name '*.parquet' 2>/dev/null | head -1" 2>/dev/null | grep -q ".parquet"; then
    echo -e "${YELLOW}Warning: No Parquet files found in /output/parquet/validated yet.${NC}"
    echo -e "${YELLOW}         Run Phase 1 ingestion locally first if this is your first run.${NC}"
fi


# 5. Deploy Dask scheduler + workers (or restart them if already running so they
#    pick up the now-populated hostPath volume with a fresh pod start).
echo -e "\n${GREEN}[5/6] Deploying Dask scheduler and workers...${NC}"
kubectl apply -f k8s/dask-scheduler.yaml
kubectl apply -f k8s/dask-worker.yaml

# Force pods to restart so they re-bind the hostPath volume from scratch.
# This is a no-op for brand-new deployments and fixes the race for re-runs.
echo -e "${YELLOW}Restarting pods to ensure they bind the fresh data mount...${NC}"
kubectl rollout restart deployment/dask-scheduler
kubectl rollout restart deployment/dask-worker

# Wait for both deployments to finish rolling out.
echo -e "${YELLOW}Waiting for Dask scheduler to be Ready...${NC}"
kubectl rollout status deployment/dask-scheduler --timeout=120s
echo -e "${YELLOW}Waiting for Dask workers to be Ready...${NC}"
kubectl rollout status deployment/dask-worker --timeout=120s

# 6. Port forward the dashboard and scheduler with a robust restart loop
echo -e "\n${GREEN}[6/6] Port-forwarding Dask dashboard (8787) and scheduler (8786)...${NC}"
# Kill existing port-forwards to avoid conflict
pkill -f "port-forward svc/dask-scheduler" || true

# Run port-forwards in a while loop so they reconnect if they get dropped
(while true; do kubectl port-forward svc/dask-scheduler 8787:8787; sleep 2; done) > /dev/null 2>&1 &
(while true; do kubectl port-forward svc/dask-scheduler 8786:8786; sleep 2; done) > /dev/null 2>&1 &

# Wait for port-forwarding to be ready
echo -e "${YELLOW}Waiting for port-forwarding to be ready...${NC}"
PF_TIMEOUT=30
PF_ELAPSED=0
PF_READY=0
while [ $PF_ELAPSED -lt $PF_TIMEOUT ]; do
    if (echo > /dev/tcp/localhost/8786) >/dev/null 2>&1 && (echo > /dev/tcp/localhost/8787) >/dev/null 2>&1; then
        PF_READY=1
        echo -e "${GREEN}  Port-forwarding confirmed: 8786 and 8787 are listening.${NC}"
        break
    fi
    sleep 2
    PF_ELAPSED=$((PF_ELAPSED + 2))
done
if [ $PF_READY -eq 0 ]; then
    echo -e "${RED}Warning: Port-forwarding might not be ready. Check background processes with 'ps aux | grep port-forward'.${NC}"
fi

echo -e "\n${CYAN}=== Setup Complete ===${NC}"
echo "Useful commands:"
echo "  Watch pods:      kubectl get pods -w"
echo "  Stream logs:     kubectl logs -f deployment/dask-worker"
echo "  Dashboard URL:   http://localhost:8787"
echo "  Scheduler URL:   tcp://localhost:8786"
