set shell := ["powershell.exe", "-Command"]

# Default recipe: list all commands
default:
    just --list

# Sync dependencies using uv
sync:
    uv sync

# Download a ~100MB sample of the data
data-sample:
    uv run python scripts/download_data.py --sample

# Download the full dataset (WARNING: multi-GB)
data-full:
    uv run python scripts/download_data.py

# Run the complete data pipeline
pipeline:
    uv run python pipeline/run_pipeline.py

# Launch the interactive Jupyter notebook
notebook:
    uv run jupyter notebook notebooks/pipeline.ipynb

# Save output figures to the /output/figures directory
save-figures:
    uv run python scripts/save_figures.py

# Start Minikube execution
start-minikube:
    pwsh -File scripts/start_minikube.ps1

# Run Dask Pipeline
run-dask:
    pwsh -File scripts/run_dask.ps1

# Run Spark Pipeline
run-spark:
    pwsh -File scripts/run_spark.ps1

# Check the generated schema for valid parquet files
check-schema:
    uv run python scripts/check_schema.py

# Clean temporary output and data files
clean:
    rm -rf output/parquet/*
    rm -rf output/results/*
    @echo "Cleaned output directories."
