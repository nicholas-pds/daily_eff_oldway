# src/main_midday.py
from pathlib import Path
from datetime import datetime, time
import pandas as pd

from .db_handler import execute_sql_to_dataframe
from .sheets_handler import SheetsHandler


def main():
    """Midday efficiency update — runs at noon, includes 3 AM to 12 PM today."""
    
    # --- Paths ---
    BASE_DIR = Path(__file__).parent
    SQL_FILE_PATH = BASE_DIR.parent / "sql_query" / "task_by_tech_eff.sql"
    
    # --- Config ---
    SHEET_NAME = "12PM_MIDDAY_IMPORT"

    print(f"Midday run started at {datetime.now():%Y-%m-%d %H:%M}")
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
    # Step 2: Filter to TODAY between 3:00 AM and 12:00 PM (noon)
    # ================================================================
    print("\nStep 2: Filtering to today's data (3:00 AM – 12:00 PM)...")

    DATE_COL = 'CompleteDate'

    if DATE_COL not in data_df.columns:
        print(f"ERROR: Column '{DATE_COL}' not found!")
        print("Available columns:", list(data_df.columns))
        return

    # Ensure completedate is datetime (not string)
    data_df[DATE_COL] = pd.to_datetime(data_df[DATE_COL], errors='coerce')

    # Drop any rows that failed to parse
    bad_dates = data_df[DATE_COL].isna()
    if bad_dates.any():
        print(f"   Dropped {bad_dates.sum()} rows with invalid completedate")
        data_df = data_df[~bad_dates]

    # Define time window for TODAY
    today = datetime.now().date()
    start_time = datetime.combine(today, time(3, 0))   # 3:00 AM today
    end_time   = datetime.combine(today, time(12, 0))  # 12:00 PM today

    print(f"   → Looking for completions from {start_time.strftime('%Y-%m-%d %I:%M %p')} "
          f"to {end_time.strftime('%I:%M %p')}")

    # Filter: today AND within time range
    mask = (data_df[DATE_COL] >= start_time) & (data_df[DATE_COL] <= end_time)
    before = len(data_df)
    data_df = data_df[mask].copy()
    after = len(data_df)

    print(f"   → Filtered: {before:,} → {after:,} rows in 3 AM – noon window")

    if after == 0:
        print("   No tasks completed yet in the 3 AM – 12 PM window today.")

    # ================================================================
    # Step 3: Upload to Google Sheets (midday tab)
    # ================================================================
    print("\nStep 3: Uploading midday data to Google Sheets...")

    try:
        sheets = SheetsHandler()
        success = sheets.write_dataframe_to_sheet(
            df=data_df,
            sheet_name=SHEET_NAME,
            clear_sheet=True
        )

        if success:
            print(f"SUCCESS: Midday update complete! "
                  f"Uploaded {after:,} rows (3 AM – noon) to '{SHEET_NAME}'")
        else:
            print("Upload failed (SheetsHandler returned False)")

    except Exception as e:
        print(f"ERROR during upload: {e}")

    print(f"\nMidday run finished at {datetime.now():%H:%M:%S}\n")


if __name__ == "__main__":
    main()