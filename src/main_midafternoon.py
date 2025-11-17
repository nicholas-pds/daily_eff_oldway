# src/main_midafternoon.py
from pathlib import Path
from datetime import datetime, time
import pandas as pd

from .db_handler import execute_sql_to_dataframe
from .sheets_handler import SheetsHandler


def main():
    """Mid-afternoon efficiency update — 3:00 AM to 3:00 PM today."""
    
    # --- Paths ---
    BASE_DIR = Path(__file__).parent
    SQL_FILE_PATH = BASE_DIR.parent / "sql_query" / "task_by_tech_eff.sql"
    
    # --- Config ---
    SHEET_NAME = "3PM_MIDDAY_IMPORT" 

    print(f"Mid-afternoon run started at {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Loading SQL from: {SQL_FILE_PATH}")

    # Step 1: Run query
    try:
        data_df = execute_sql_to_dataframe(str(SQL_FILE_PATH))
    except Exception as e:
        print(f"ERROR loading data: {e}")
        return

    if data_df.empty:
        print("No data returned.")
        return

    print(f"Query successful → {len(data_df):,} total rows retrieved")

    # ================================================================
    # Step 2: Filter to TODAY between 3:00 AM and 3:00 PM
    # ================================================================
    print("\nStep 2: Filtering to today's data (3:00 AM – 3:00 PM)...")

    DATE_COL = 'CompleteDate'

    if DATE_COL not in data_df.columns:
        print(f"ERROR: Column '{DATE_COL}' not found!")
        print("Available columns:", list(data_df.columns))
        return

    # Convert to full datetime
    data_df[DATE_COL] = pd.to_datetime(data_df[DATE_COL], errors='coerce')

    # Drop invalid dates
    bad = data_df[DATE_COL].isna()
    if bad.any():
        print(f"   Dropped {bad.sum()} rows with invalid completedate")
        data_df = data_df[~bad]

    # Define today's time window
    today = datetime.now().date()
    start_time = datetime.combine(today, time(3, 0))   # 3:00 AM
    end_time   = datetime.combine(today, time(15, 0))  # 3:00 PM (15:00 in 24h)

    print(f"   → Including completions from {start_time:%I:%M %p} to {end_time:%I:%M %p} today")

    # Filter
    mask = (data_df[DATE_COL] >= start_time) & (data_df[DATE_COL] <= end_time)
    before = len(data_df)
    data_df = data_df[mask].copy()
    after = len(data_df)

    print(f"   → Filtered: {before:,} → {after:,} rows (3 AM – 3 PM)")

    if after == 0:
        print("   No tasks completed yet in the 3 AM – 3 PM window.")

    # ================================================================
    # Step 3: Upload to Google Sheets
    # ================================================================
    print("\nStep 3: Uploading 3 PM update to Google Sheets...")

    try:
        sheets = SheetsHandler()
        success = sheets.write_dataframe_to_sheet(
            df=data_df,
            sheet_name=SHEET_NAME,
            clear_sheet=True
        )

        if success:
            print(f"SUCCESS: 3 PM update complete! "
                  f"Uploaded {after:,} rows to '{SHEET_NAME}'")
        else:
            print("Upload reported failure.")

    except Exception as e:
        print(f"ERROR during upload: {e}")

    print(f"\nMid-afternoon run finished at {datetime.now():%H:%M:%S}\n")


if __name__ == "__main__":
    main()