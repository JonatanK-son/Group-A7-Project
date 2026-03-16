# Data Management

This project handles large e-commerce behavior datasets.

## Dataset Source
The data is sourced from [Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store). It represents user behavior from a multi-category store over several months.

## Download Utility
We provide a script to easily download the data.

### Sample Data (~100MB)
For local development and testing, you can download a small sample of the `2019-Oct.csv` file:

```bash
uv run python scripts/download_data.py --sample
```

This downloads roughly 100MB using HTTP Range requests from a Hugging Face mirror and truncates it to the last full line to ensure the CSV structure is preserved.

### Full Dataset (Multi-GB)
To download the full October 2019 file:

```bash
uv run python scripts/download_data.py
```
> [!WARNING]
> The full dataset is over 5GB for a single month. Ensure you have sufficient disk space.

## Structure
Files should be placed in the `data/` directory:
- `data/2019-Oct.csv`
- `data/2019-Nov.csv` (optional)

The pipeline expects these files to be present for the ingestion phase.
