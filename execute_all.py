import shutil
import time
import duckdb
from pathlib import Path
from ingest_7_day_data import extract_seven_day_data
from ingest_daily_data import extract_daily_data
from ingest_weekly_data import extract_weekly_data
from extract_data import fetch_migrant_data

def execute_all():

    p = Path()
    incoming_path = p / 'incoming'
    data_path = p / 'data'

    # extract file
    fetch_migrant_data()
    time.sleep(5)

    # ingest data
    extract_seven_day_data()
    extract_daily_data()
    extract_weekly_data()

    # move incoming file
    source_dir = None
    target_dir = None
    for f in incoming_path.glob('*.ods'):
        source_dir = incoming_path / f.name
        target_dir = data_path / f.name

    # update motherduck
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImFkYW1fbG93ZUBpY2xvdWQuY29tIiwic2Vzc2lvbiI6ImFkYW1fbG93ZS5pY2xvdWQuY29tIiwicGF0IjoiTWsxbGx0aWsyd2t4ME03ek5kWDZVRnhGenN3VDBrelF5RnZYNlVucWtFayIsInVzZXJJZCI6IjA1M2NkYjRkLWZhNjgtNDUwMi1hYzViLWY4YTQ2ZDgwZGE5OSIsImlzcyI6Im1kX3BhdCIsInJlYWRPbmx5IjpmYWxzZSwidG9rZW5UeXBlIjoicmVhZF93cml0ZSIsImlhdCI6MTc1NTI3MjgxMn0.fA0kLOb-f60IDruW6_we0TFadk24nj6KxrqXWVquBDE"
    con = duckdb.connect("migrant_crossings_db.duckdb")
    con.execute(f"""
    ATTACH 'md:?motherduck_token={token}';
    """)

    con.execute("""
    CREATE OR REPLACE DATABASE migrant_crossings FROM 'migrant_crossings_db.duckdb';
    """)

    con.close()

    try:
      shutil.move(source_dir, target_dir)
    except Exception as e:
        pass

if __name__ == "__main__":
    execute_all()