import os
import json # Added import for JSON handling
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe
from dotenv import load_dotenv

# Load environment variables from .env file (assuming they are set externally, e.g., in a runner)
load_dotenv()

class SheetsHandler:
    def __init__(self):
        """
        Initialize Google Sheets handler with credentials from environment variables.
        """
        self.client = None
        self._authenticate()
    
    def _authenticate(self):
        """
        Authenticate with Google Sheets API using service account credentials
        read directly from the GOOGLE_SERVICE_ACCOUNT_JSON environment variable.
        """
        try:
            # 1. Get credentials JSON string from environment variable
            credentials_json_string = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

            if not credentials_json_string:
                raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not found in environment variables.")
            
            # 2. Parse the JSON string into a Python dictionary (credentials info)
            try:
                credentials_info = json.loads(credentials_json_string)
            except json.JSONDecodeError:
                raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON is not a valid JSON string. Check formatting.")

            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # 3. Authenticate using the dictionary (info) instead of a file
            creds = Credentials.from_service_account_info(
                credentials_info, 
                scopes=scopes
            )
            self.client = gspread.authorize(creds)
            print("✅ Successfully authenticated with Google Sheets API using service account JSON")
            
        except Exception as e:
            print(f"🚨 ERROR authenticating with Google Sheets: {e}")
            # Re-raise the error so the calling application knows initialization failed
            raise
    
    def write_dataframe_to_sheet(self, df, sheet_name, clear_sheet=True):
        """
        Write a pandas DataFrame to a specific sheet/tab in a Google Spreadsheet.
        Uses SPREADSHEET_ID from environment variables.
        
        Args:
            df (pd.DataFrame): The DataFrame to write
            sheet_name (str): The name of the sheet/tab to write to
            clear_sheet (bool): Whether to clear existing content first
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Get spreadsheet ID from environment variable
            spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
            
            if not spreadsheet_id:
                raise ValueError("GOOGLE_SPREADSHEET_ID not found in environment variables")
            
            # Open the spreadsheet
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            
            # Try to get existing worksheet, or create if it doesn't exist
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
                print(f"Found existing sheet: '{sheet_name}'")
            except gspread.exceptions.WorksheetNotFound:
                # Calculate required rows and columns for new sheet creation
                rows = str(len(df) + 1)
                cols = str(len(df.columns))
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name, 
                    rows=rows, 
                    cols=cols
                )
                print(f"Created new sheet: '{sheet_name}'")
            
            # Clear existing content if requested
            if clear_sheet:
                worksheet.clear()
                print(f"Cleared existing content in '{sheet_name}'")
            
            # Write DataFrame to sheet
            set_with_dataframe(worksheet, df, include_index=False, include_column_header=True)
            
            print(f"✅ Successfully wrote {len(df)} rows to '{sheet_name}' tab")
            return True
            
        except Exception as e:
            print(f"🚨 ERROR writing to Google Sheets: {e}")
            return False

    def read_sheet_to_dataframe(self, sheet_name):
        """
        Reads data from a specific sheet/tab into a pandas DataFrame.
        Uses SPREADSHEET_ID from environment variables.
        
        Args:
            sheet_name (str): The name of the sheet/tab to read from.
            
        Returns:
            pd.DataFrame or None: The DataFrame containing the sheet data, or None if reading fails.
        """
        try:
            spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
            
            if not spreadsheet_id:
                raise ValueError("GOOGLE_SPREADSHEET_ID not found in environment variables")
            
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            
            # Get all values from the worksheet
            data = worksheet.get_all_values()
            
            if not data:
                print(f"⚠️ Sheet '{sheet_name}' is empty.")
                return pd.DataFrame()

            # The first row is the header
            headers = data[0]
            # The rest is the data
            df = pd.DataFrame(data[1:], columns=headers)
            
            print(f"✅ Successfully read {len(df)} rows from '{sheet_name}' tab")
            return df
            
        except Exception as e:
            print(f"🚨 ERROR reading from Google Sheets: {e}")
            return None
    
    def update_dataframe_to_sheet(self, df, sheet_name, start_cell='A1'):
        """
        Update a specific range in a sheet with DataFrame data.
        Uses SPREADSHEET_ID from environment variables.
        
        Args:
            df (pd.DataFrame): The DataFrame to write
            sheet_name (str): The name of the sheet/tab to write to
            start_cell (str): Starting cell (e.g., 'A1', 'B2')
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID")
            
            if not spreadsheet_id:
                raise ValueError("GOOGLE_SPREADSHEET_ID not found in environment variables")
            
            spreadsheet = self.client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            
            # Helper to convert Excel-style column letter to 1-based index (A=1, B=2, ...)
            col_index = ord(start_cell[0].upper()) - 64
            # Helper to parse row number (assuming single letter column, e.g., 'A1')
            try:
                row_index = int(start_cell[1:])
            except ValueError:
                 raise ValueError("Invalid start_cell format. Expected format like 'A1'.")

            # Update starting from specific cell
            set_with_dataframe(
                worksheet, 
                df, 
                include_index=False, 
                include_column_header=True,
                row=row_index,
                col=col_index
            )
            
            print(f"✅ Successfully updated '{sheet_name}' starting at {start_cell}")
            return True
            
        except Exception as e:
            print(f"🚨 ERROR updating Google Sheets: {e}")
            return False

if __name__ == "__main__":
    # --- DEMONSTRATION & SETUP CHECK ---
    print("\n--- SheetsHandler Demonstration ---")
    
    # NOTE: You MUST set the following environment variables in your runner or .env file:
    # 1. GOOGLE_SERVICE_ACCOUNT_JSON (The full JSON content as a single string)
    # 2. GOOGLE_SPREADSHEET_ID (The unique ID from the spreadsheet URL, e.g., 12345...)
    # 3. GOOGLE_SHEET_NAME (The tab name you want to use, e.g., 'Airway Hold Cases')

    # Example data for demonstration
    data = {
        'Timestamp': [pd.Timestamp.now()] * 3,
        'Metric': ['Revenue', 'Users', 'Conversions'],
        'Value': [1500, 25000, 320]
    }
    test_df = pd.DataFrame(data)
    
    # Use the environment variable for the sheet name, defaulting to a test value
    test_sheet_name = os.getenv("GOOGLE_SHEET_NAME", "Test_Data_Tab")

    try:
        # Initialize handler (This performs authentication using GOOGLE_SERVICE_ACCOUNT_JSON)
        handler = SheetsHandler()

        # 1. Write Data Demo (Clears sheet content before writing)
        print("\n--- 1. Writing DataFrame ---")
        if os.getenv("GOOGLE_SPREADSHEET_ID"):
             handler.write_dataframe_to_sheet(
                test_df, 
                test_sheet_name, 
                clear_sheet=True
            )
        else:
            print("Skipping Write Demo: GOOGLE_SPREADSHEET_ID environment variable is not set.")

        # 2. Read Data Demo
        print("\n--- 2. Reading DataFrame ---")
        if os.getenv("GOOGLE_SPREADSHEET_ID"):
            read_df = handler.read_sheet_to_dataframe(test_sheet_name)
            if read_df is not None and not read_df.empty:
                print("Read DataFrame head:")
                print(read_df.head())
        else:
            print("Skipping Read Demo: GOOGLE_SPREADSHEET_ID environment variable is not set.")


        # 3. Update Data Demo 
        print("\n--- 3. Updating DataFrame (starting at A1) ---")
        if os.getenv("GOOGLE_SPREADSHEET_ID"):
            update_df = pd.DataFrame({'New_Update_Col': ['Updated!']})
            handler.update_dataframe_to_sheet(
                 update_df, 
                 test_sheet_name, 
                 start_cell='A1' # Overwrites cell A1 and A2 with this new data and header
            )
        else:
            print("Skipping Update Demo: GOOGLE_SPREADSHEET_ID environment variable is not set.")


    except ValueError as e:
        print(f"\n--- CRITICAL SETUP ERROR ---")
        print(f"The program halted due to a missing environment variable: {e}")
        print("Please ensure GOOGLE_SERVICE_ACCOUNT_JSON and GOOGLE_SPREADSHEET_ID are set correctly in your environment.")
    except Exception as e:
        print(f"\n--- GENERAL ERROR ---")
        print(f"An unexpected error occurred during the demonstration: {e}")