import os
import requests
import zipfile
import io
import pandas as pd
import numpy as np  # Import numpy
from supabase import create_client, Client
from dotenv import load_dotenv
import time  # Import the time module for adding delays

# --- Configuration ---
load_dotenv()  # Load environment variables from .env file

# The direct download link for the DSLD database zip file
DOWNLOAD_URL = "https://api.ods.od.nih.gov/dsld/s3/data/DSLD-full-database-CSV.zip"
TARGET_FILENAME_CONTAINS = "ProductOverview"  # We'll look for files with this in their name

# Supabase configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: Supabase URL and Key must be set in your .env file.")
    exit()


# --- Main Script Logic ---

def download_and_unzip_data(url: str) -> list:
    """
    Downloads a zip file from a URL, unzips it in memory,
    and returns a list of file-like objects for the target CSVs.
    """
    print(f"Downloading data from {url}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raises an error for bad status codes (4xx or 5xx)

        zip_file = zipfile.ZipFile(io.BytesIO(response.content))

        csv_files = []
        for file_info in zip_file.infolist():
            if TARGET_FILENAME_CONTAINS in file_info.filename and file_info.filename.endswith('.csv'):
                print(f"Found matching file: {file_info.filename}")
                # Open the file from the zip archive and add it to our list
                csv_files.append(zip_file.open(file_info.filename))

        if not csv_files:
            print("No 'ProductOverview' CSV files found in the zip archive.")
            return []

        return csv_files

    except requests.exceptions.RequestException as e:
        print(f"Error downloading the file: {e}")
        return []
    except zipfile.BadZipFile:
        print("Error: The downloaded file is not a valid zip file.")
        return []


def combine_csvs_to_dataframe(csv_files: list) -> pd.DataFrame:
    """
    Takes a list of CSV file objects, reads them into Pandas DataFrames,
    and concatenates them into a single DataFrame.
    """
    if not csv_files:
        return pd.DataFrame()

    print("Combining CSV files into a single DataFrame...")

    # Read each CSV file into a DataFrame
    dataframes = [pd.read_csv(file, low_memory=False) for file in csv_files]

    # Combine all DataFrames into one
    combined_df = pd.concat(dataframes, ignore_index=True)

    # IMPORTANT: The column names in the CSV must match the column names in your SQL table.
    # We will rename them here to ensure they match the quoted identifiers in your Supabase table.
    # This step is crucial for the upload to work correctly.
    column_mapping = {
        'URL': 'URL',
        'DSLD ID': 'DSLD ID',
        'Product Name': 'Product Name',
        'Brand Name': 'Brand Name',
        'Bar Code': 'Bar Code',
        'Net Contents': 'Net Contents',
        'Serving Size': 'Serving Size',
        'Product Type [LanguaL]': 'Product Type [LanguaL]',
        'Supplement Form [LanguaL]': 'Supplement Form [LanguaL]',
        'Date Entered into DSLD': 'Date Entered into DSLD',
        'Market Status': 'Market Status',
        'Suggested Use': 'Suggested Use'
    }
    combined_df.rename(columns=column_mapping, inplace=True)

    print(f"Successfully combined data. Total rows: {len(combined_df)}")
    return combined_df


def upload_dataframe_to_supabase(df: pd.DataFrame, supabase_client: Client):
    """
    Uploads the contents of a Pandas DataFrame to the 'dsld_supplements' table in batches.
    """
    if df.empty:
        print("DataFrame is empty. Nothing to upload.")
        return

    print("Preparing to upload data to Supabase in batches...")

    try:
        # Clean the data before uploading
        df_cleaned = df.replace({pd.NA: None, np.nan: None})
        records = df_cleaned.to_dict(orient='records')

        batch_size = 500  # You can adjust this number if needed
        total_records = len(records)

        for i in range(0, total_records, batch_size):
            batch = records[i:i + batch_size]
            print(
                f"Uploading batch {i // batch_size + 1} of {(total_records + batch_size - 1) // batch_size}... ({len(batch)} records)")

            # --- KEY CHANGE: Modern Error Handling ---
            # In newer versions of supabase-py, errors are raised as exceptions.
            # The .execute() call will throw an error if the batch fails,
            # which will be caught by the 'except' block below.
            # The `if response.error:` check has been removed.
            response = supabase_client.table('dsld_supplements').upsert(batch, on_conflict='"DSLD ID"').execute()

            # Optional: Check the response data if needed
            if response.data:
                print(f"Batch {i // batch_size + 1} uploaded successfully.")

            time.sleep(1)  # Add a small delay between batches to be nice to the server

        print("All batches uploaded successfully.")

    except Exception as e:
        # This block will now correctly catch any API errors from Supabase.
        print(f"An error occurred during the Supabase upload: {e}")


def main():
    """
    Main function to orchestrate the download, processing, and upload.
    """
    # 1. Download and extract the data
    csv_file_objects = download_and_unzip_data(DOWNLOAD_URL)

    if not csv_file_objects:
        print("Script finished due to no files being found.")
        return

    # 2. Combine the CSVs into a single DataFrame
    product_dataframe = combine_csvs_to_dataframe(csv_file_objects)

    # 3. Initialize Supabase client and upload the data
    supabase_client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    upload_dataframe_to_supabase(product_dataframe, supabase_client)

    print("Script finished.")


if __name__ == "__main__":
    main()
