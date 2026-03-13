import json
from pathlib import Path

p = Path("notebooks/pipeline.ipynb")
with open(p, "r", encoding="utf-8") as f:
    text = f.read()

# Instead of parsing everything and guessing arrays, just do a string replace on the source.
# The user's notebook actually contains these exact strings in the source array.

# We just want to replace the Spark save lines.
old_block1 = r'    "    # Persist Spark results as Parquet\n",'
old_block2 = r'    "    results_fwd = str(RESULTS_DIR).replace(\'\\\\\\\\\', \'/\')\n",'
old_block3 = r'    "    spark_results[\'revenue\'].write.mode(\'overwrite\').parquet(f\'{results_fwd}/spark_revenue\')\n",'
old_block4 = r'    "    spark_results[\'funnel\'].write.mode(\'overwrite\').parquet(f\'{results_fwd}/spark_funnel\')\n",'
old_block5 = r'    "    spark_results[\'window\'].write.mode(\'overwrite\').parquet(f\'{results_fwd}/spark_window_rank\')\n",'
old_block6 = r'    "    spark_results[\'hourly\'].write.mode(\'overwrite\').parquet(f\'{results_fwd}/spark_hourly\')\n",'

# NOTE: Since the previous code in the active session might have been 'file:///'... wait,
# git checkout reverted to HEAD which has str(RESULTS_DIR).replace('\\', '/')

new_block1 = r'    "    # Persist Spark results via Pandas to avoid Py4J Windows path overwrite bugs\n",'
new_block2 = r'    "    save_parquet_pandas(spark_results[\'revenue\'].toPandas(), RESULTS_DIR / \'spark_revenue.parquet\')\n",'
new_block3 = r'    "    save_parquet_pandas(spark_results[\'funnel\'].toPandas(),  RESULTS_DIR / \'spark_funnel.parquet\')\n",'
new_block4 = r'    "    save_parquet_pandas(spark_results[\'window\'].toPandas(),  RESULTS_DIR / \'spark_window_rank.parquet\')\n",'
new_block5 = r'    "    save_parquet_pandas(spark_results[\'hourly\'].toPandas(),  RESULTS_DIR / \'spark_hourly.parquet\')\n"'

# Actually, the best way to do this reliably given escaping is to parse the JSON, find the target cell and rewrite its 'source' cleanly without regex matching JSON artifacts
import json

with open(p, "r", encoding="utf-8") as f:
    nb = json.load(f)

for cell in nb.get("cells", []):
    source = cell.get("source", [])
    if any("spark_results['revenue'].write.mode" in line or "spark_results['revenue']" in line for line in source):
        # We found the spark block
        new_source = []
        skip = False
        for line in source:
            if "# Persist Spark results as Parquet" in line:
                new_source.append("    # Persist Spark results via Pandas to avoid Py4J Windows path overwrite bugs\n")
            elif "results_fwd =" in line:
                pass # skip
            elif "spark_results['revenue'].write" in line:
                new_source.append("    save_parquet_pandas(spark_results['revenue'].toPandas(), RESULTS_DIR / 'spark_revenue.parquet')\n")
            elif "spark_results['funnel'].write" in line:
                new_source.append("    save_parquet_pandas(spark_results['funnel'].toPandas(), RESULTS_DIR / 'spark_funnel.parquet')\n")
            elif "spark_results['window'].write" in line:
                new_source.append("    save_parquet_pandas(spark_results['window'].toPandas(), RESULTS_DIR / 'spark_window_rank.parquet')\n")
            elif "spark_results['hourly'].write" in line:
                new_source.append("    save_parquet_pandas(spark_results['hourly'].toPandas(), RESULTS_DIR / 'spark_hourly.parquet')\n")
            else:
                new_source.append(line)
        cell["source"] = new_source

with open(p, "w", encoding="utf-8") as f:
    json.dump(nb, f, indent=1)
