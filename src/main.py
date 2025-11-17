# src/main.py
from pathlib import Path
from datetime import date
import pandas as pd

from .db_handler import execute_sql_to_dataframe
from .sheets_handler import SheetsHandler
from .holidays import previous_business_day


def main():
    """Main function to orchestrate the daily process."""
    
    # --- Paths ---
    BASE_DIR = Path(__file__).parent
    SQL_FILE_PATH = BASE_DIR.parent / "sql_query" / "task_by_tech_eff.sql"
    
    # --- Config ---
    SHEET_NAME = "MagicTouch A_EFF Tasks Report"

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

    print(f"Query successful → {len(data_df):,} rows retrieved")

    # ================================================================
    # Step 2: Filter to previous business day using the real column name
    # ================================================================
    print("\nStep 2: Filtering to previous business day...")

    target_date = previous_business_day()
    print(f"   → Previous business day: {target_date} ({target_date:%A, %B %d, %Y})")

    # Your actual column name is 'completedate' (lowercase)
    DATE_COL = 'CompleteDate'

    if DATE_COL not in data_df.columns:
        print(f"ERROR: Column '{DATE_COL}' not found!")
        print("Available columns:", list(data_df.columns))
        return

    # Convert to date only (strips the time part safely)
    data_df[DATE_COL] = pd.to_datetime(data_df[DATE_COL]).dt.date

    # Filter
    before = len(data_df)
    data_df = data_df[data_df[DATE_COL] == target_date].copy()
    after = len(data_df)

    print(f"   → Filtered from {before:,} → {after:,} rows for {target_date}")

    if after == 0:
        print("   No records for the previous business day.")

    # ================================================================
    # Step 3: Upload to Google Sheets
    # ================================================================
    print("\nStep 3: Uploading to Google Sheets...")

    try:
        sheets = SheetsHandler()
        success = sheets.write_dataframe_to_sheet(
            df=data_df,
            sheet_name=SHEET_NAME,
            clear_sheet=True
        )

        if success:
            print(f"SUCCESS: Uploaded {after:,} rows for {target_date} to '{SHEET_NAME}' sheet!")
        else:
            print("Upload failed (SheetsHandler returned False)")

    except Exception as e:
        print(f"ERROR during upload: {e}")

    print("\nDaily run complete!\n")


if __name__ == "__main__":
    main()