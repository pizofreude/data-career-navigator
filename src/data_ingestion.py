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
from kaggle.api.kaggle_api_extended import KaggleApi

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
    dest_folder="data/bronze" 
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
    # Download using Kaggle CLI
    cmd = [
        "kaggle", "datasets", "download", dataset,
        "-f", filename, "--unzip", "-p", dest_folder
    ]
    print(f"Running command: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        print(f"Downloaded {filename} to {dest_folder}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")
        print("Ensure Kaggle credentials are correctly configured and the dataset is accessible.")
        raise

if __name__ == "__main__":
    test_kaggle_credentials()
    download_kaggle_csv()