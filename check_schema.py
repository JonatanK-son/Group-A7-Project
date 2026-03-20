import pyarrow.parquet as pq
from pathlib import Path

path = Path("output/parquet/validated")
files = list(path.rglob("*.parquet"))
schemas = set()
for f in files:
    schema = pq.read_metadata(f).schema
    for i in range(len(schema)):
        col = schema[i]
        if col.name == "event_time":
            schemas.add(str(col))

for s in schemas:
    print(s)
