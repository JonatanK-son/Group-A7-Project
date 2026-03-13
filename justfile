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

# Clean temporary output and data files
clean:
    rm -rf output/parquet/*
    rm -rf output/results/*
    @echo "Cleaned output directories."
