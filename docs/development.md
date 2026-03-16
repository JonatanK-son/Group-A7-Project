# Local Development Setup

This guide explains how to set up the project locally for development.

## Prerequisites
- [uv](https://github.com/astral-sh/uv) installed on your system.
- Python 3.12 or later.

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd Group-A7-Project
   ```

2. **Sync dependencies**:
   `uv` will automatically create a virtual environment and install all dependencies.
   ```bash
   uv sync
   ```

3. **Download sample data**:
   ```bash
   uv run python scripts/download_data.py --sample
   ```

## Running the Pipeline

### Standalone Script
To run the full pipeline:
```bash
uv run pipeline/run_pipeline.py
```

### Jupyter Notebook
To work interactively:
```bash
uv run jupyter notebook notebooks/pipeline.ipynb
```

## Development Shortcuts (just)

We use `just` as a command runner. Available commands:

| Command | Action |
|---|---|
| `just` | List all available commands |
| `just sync` | Sync dependencies and set up environment |
| `just data-sample` | Download ~100MB data sample |
| `just data-full` | Download full dataset |
| `just pipeline` | Run the full data pipeline |
| `just notebook` | Launch Jupyter notebook |
| `just clean` | Clean output and data directories |

## Environment Management
- Add new dependencies: `uv add <package>`
- Update dependencies: `uv sync` or `just sync`
- Run a script in the venv: `uv run <script.py>`
