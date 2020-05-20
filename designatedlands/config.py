# pick up db_url from $DATABASE_URL if available
import os

if "DATABASE_URL" in os.environ:
    db_url = os.environ["DATABASE_URL"]
else:
    db_url = (
        "postgresql://designatedlands:designatedlands@localhost:5432/designatedlands"
    )


defaultconfig = {
    "dl_path": "source_data",
    "sources_designations": "sources_designations.csv",
    "sources_supporting": "sources_supporting.csv",
    "out_path": "outputs",
    "db_url": db_url,
    "n_processes": -1,
    "resolution": 10,
}
