FROM python:3.12-slim

WORKDIR /app

# Install Java for PySpark
RUN apt-get update && apt-get install -y default-jre-headless && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/       src/
COPY pipeline/  pipeline/

ENV PYTHONPATH=/app

CMD ["python", "pipeline/run_pipeline.py"]
