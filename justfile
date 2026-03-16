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

# Build the docker image for the pipeline (requires requirements.txt)
docker-build:
    uv export --format requirements-txt > requirements.txt
    docker build -t e-commerce-pipeline .

# Run the pipeline inside the docker container
docker-pipeline: docker-build
    docker run --rm -v "{{justfile_directory()}}\data:/app/data" -v "{{justfile_directory()}}\output:/app/output" e-commerce-pipeline

# Launch the interactive Jupyter notebook
notebook:
    uv run jupyter notebook notebooks/pipeline.ipynb

# Clean temporary output and data files
clean:
    rm -rf output/parquet/*
    rm -rf output/results/*
    @echo "Cleaned output directories."
