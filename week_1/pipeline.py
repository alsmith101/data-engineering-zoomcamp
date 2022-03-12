import pandas as pd
import argparse
from datetime import datetime

todays_date = datetime.now().date()

parser = argparse.ArgumentParser("pipeline", description="my data pipeline")
parser.add_argument(
    "--start-date",
    required=True,
    type=lambda start_date: datetime.strptime(start_date, '%Y-%m-%d').date()
)
parser.add_argument(
    "--end-date",
    default=todays_date
)

args = parser.parse_args()

print(f"start_date={args.start_date}")
print(f"end_date={args.end_date}")

print("File ran successfully")