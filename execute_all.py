import shutil
import time
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

    try:
      shutil.move(source_dir, target_dir)
    except Exception as e:
        pass

if __name__ == "__main__":
    execute_all()