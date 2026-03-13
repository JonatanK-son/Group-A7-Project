import json
from pathlib import Path

p = Path("notebooks/pipeline.ipynb")
with open(p, "r", encoding="utf-8") as f:
    d = json.load(f)

for cell in d.get("cells", []):
    source = cell.get("source", [])
    if any("spark_results" in line for line in source) and any("results_fwd" in line for line in source):
        # We found the target cell.
        new_source = []
        for line in source:
            if "results_fwd = str(RESULTS_DIR).replace" in line:
                new_source.append("    results_fwd = 'file:///' + str(RESULTS_DIR.resolve()).replace('\\\\', '/')\n")
            else:
                new_source.append(line)
        cell["source"] = new_source

with open(p, "w", encoding="utf-8") as f:
    json.dump(d, f, indent=1)
