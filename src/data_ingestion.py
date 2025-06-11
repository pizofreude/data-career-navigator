"""
File: src/data_ingestion.py 
----------------------------
Automated data ingestion script for Data Career Navigator.
Downloads the latest 'clean_jobs.csv' from Kaggle and saves it to the data/bronze directory.
Intended to be run monthly via GitHub Actions.
"""

# Import necessary libraries
import os
import subprocess
import shutil
from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd

def test_kaggle_credentials():
    """
    Verifies that Kaggle credentials are valid by attempting to authenticate.

    If the credentials are invalid, this function will raise an exception.

    This function is intended to be used as a sanity check before attempting
    to download data from Kaggle.
    """
    api = KaggleApi()
    api.authenticate()
    print("Kaggle credentials are valid.")

def download_kaggle_csv(
    dataset="joykimaiyo18/linkedin-data-jobs-dataset",
    filename="clean_jobs.csv",
    dest_folder="data/bronze",
    output_filename="clean_jobs_latest.csv"
):
    """
    Downloads a CSV file from a Kaggle dataset using the Kaggle API.

    Args:
        dataset (str): Kaggle dataset identifier.
        filename (str): Name of the file to download.
        dest_folder (str): Directory to save the downloaded file.
        Our script is saving the file to ../data/bronze/clean_jobs.csv (relative to src),
        which is actually data/bronze/clean_jobs.csv at the repo root when run locally,
        but in GitHub Actions, the working directory is the repo root, so ../data/bronze
        points outside the repo.
    """
    os.makedirs(dest_folder, exist_ok=True)
    # Download using Kaggle CLI to dest_folder
    cmd = [
        "kaggle", "datasets", "download", dataset,
        "-f", filename, "--unzip", "-p", dest_folder
    ]
    print(f"Running command: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        print(f"Downloaded {filename} to {dest_folder}")
        # If the file is not already named as output_filename, rename it
        downloaded_path = os.path.join(dest_folder, filename)
        output_path = os.path.join(dest_folder, output_filename)
        if filename != output_filename:
            if os.path.exists(output_path):
                os.remove(output_path)
            os.rename(downloaded_path, output_path)
            print(f"Renamed {downloaded_path} to {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")
        print("Ensure Kaggle credentials are correctly configured and the dataset is accessible.")
        raise

# Main execution boilerplate block
if __name__ == "__main__":
    test_kaggle_credentials()
    download_kaggle_csv(output_filename="clean_jobs_latest.csv")

    master_path = os.path.join("data", "bronze", "clean_jobs.csv")
    latest_path = os.path.join("data", "bronze", "clean_jobs_latest.csv")

    # Check if master file exists
    if not os.path.exists(master_path):
        # If not, copy it from the latest file (do not remove the latest file)
        print(f"{master_path} does not exist. Creating it from {latest_path}.")
        try:
            df_latest = pd.read_csv(latest_path)
            if df_latest.empty:
                print(f"Warning: {latest_path} is empty. Master file not created.")
            else:
                shutil.copyfile(latest_path, master_path)
                print(f"Created {master_path} with {len(df_latest)} rows.")
        except Exception as e:
            print(f"Error reading {latest_path}: {e}")
        finally:
            if os.path.exists(latest_path):
                os.remove(latest_path)
                print(f"Removed temporary file {latest_path}.")
    else:
        # If master exists, append only new rows
        print(f"Appending new data from {latest_path} to {master_path} (deduplicating)...")
        try:
            df_master = pd.read_csv(master_path)
        except Exception as e:
            print(f"Error reading {master_path}: {e}")
            df_master = pd.DataFrame()
        try:
            df_latest = pd.read_csv(latest_path)
        except Exception as e:
            print(f"Error reading {latest_path}: {e}")
            df_latest = pd.DataFrame()
        if df_latest.empty:
            print(f"No new data found in {latest_path}. Nothing to append.")
        else:
            # Specify deduplication key columns if available, else all columns
            dedup_cols = None
            # Prefer 'link' or 'job_url' as deduplication key
            for col in ["link", "job_url", "job_url", "Job URL", "Job Url", "JobURL"]:
                if col in df_latest.columns and col in df_master.columns:
                    dedup_cols = [col]
                    print(f"Deduplicating on column: {col}")
                    break
            if dedup_cols is None:
                print("No unique job link column found. Deduplicating on all columns.")
            before_rows = len(df_master)
            df_combined = pd.concat([df_master, df_latest], ignore_index=True)
            df_combined = df_combined.drop_duplicates(subset=dedup_cols)
            after_rows = len(df_combined)
            new_rows = after_rows - before_rows
            df_combined.to_csv(master_path, index=False)
            print(f"Appended and deduplicated. {new_rows} new rows added. Master file now has {after_rows} rows.")
        # Remove the temporary latest file
        if os.path.exists(latest_path):
            os.remove(latest_path)
            print(f"Removed temporary file {latest_path}.")