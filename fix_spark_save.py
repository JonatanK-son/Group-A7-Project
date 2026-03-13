import json
from pathlib import Path

p = Path("notebooks/pipeline.ipynb")
with open(p, "r", encoding="utf-8") as f:
    d = json.load(f)

for cell in d.get("cells", []):
    source = cell.get("source", [])
    if any("spark_results['revenue'].write.mode" in line for line in source):
        # We found the target cell.
        new_source = []
        skip_next = False
        for line in source:
            if "results_fwd =" in line:
                new_source.append("    # Persist Spark results as Parquet (via Pandas to avoid Windows Py4J Path/Winutils bugs)\n")
            elif "spark_results['revenue'].write.mode" in line:
                new_source.append("    save_parquet_pandas(spark_results['revenue'].toPandas(), RESULTS_DIR / 'spark_revenue.parquet')\n")
            elif "spark_results['funnel'].write.mode" in line:
                new_source.append("    save_parquet_pandas(spark_results['funnel'].toPandas(),  RESULTS_DIR / 'spark_funnel.parquet')\n")
            elif "spark_results['window'].write.mode" in line:
                new_source.append("    save_parquet_pandas(spark_results['window'].toPandas(),  RESULTS_DIR / 'spark_window_rank.parquet')\n")
            elif "spark_results['hourly'].write.mode" in line:
                new_source.append("    save_parquet_pandas(spark_results['hourly'].toPandas(),  RESULTS_DIR / 'spark_hourly.parquet')\n")
            else:
                new_source.append(line)
        cell["source"] = new_source

with open(p, "w", encoding="utf-8") as f:
    json.dump(d, f, indent=1)
