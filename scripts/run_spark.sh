#!/bin/bash
# Move to the project root relative to this script
cd "$(dirname "$0")/.."
# Script to run the Spark pipeline with dedicated cluster resources
echo -e "\033[0;36m=== Preparing Spark Benchmark ===\033[0m"

# 1. Scale down Dask (to free up all cluster memory for Spark executors)
echo -e "\033[0;32mScaling down Dask workers to 0...\033[0m"
kubectl scale deployment dask-worker --replicas=0

# 2. Cleanup old Spark job
kubectl delete job spark-pipeline-job --ignore-not-found 2>/dev/null

# 3. Apply Spark RBAC (ServiceAccount, Role, RoleBinding)
echo -e "\033[0;32mApplying Spark RBAC configurations...\033[0m"
kubectl apply -f k8s/spark-role.yaml

# 4. Apply Spark Job
echo -e "\033[0;32mStarting Spark pipeline (Kubernetes Job Mode)....\033[0m"
kubectl apply -f k8s/spark-job.yaml

# 5. Wait for the Spark driver pod to be created and reach Ready state
echo -e "\033[1;33mWaiting for Spark driver pod to be created...\033[0m"
# Poll until the pod is found
until kubectl get pods -l job-name=spark-pipeline-job 2>/dev/null | grep -q 'spark-pipeline-job'; do
  sleep 1
done

echo -e "\033[1;33mWaiting for Spark driver to be Ready...\033[0m"
kubectl wait --for=condition=Ready pod -l job-name=spark-pipeline-job --timeout=60s

# Automatic Port-Forward for Spark UI (Port 4040)
kubectl port-forward job/spark-pipeline-job 4040:4040 > /dev/null 2>&1 &
echo -e "\033[0;36mSpark Dashboard: http://localhost:4040\033[0m"

kubectl logs -f job/spark-pipeline-job

echo -e "\033[0;36m\nSpark run completed.\033[0m"
