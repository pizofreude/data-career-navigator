"""
File: src/data_ingestion.py 
----------------------------
Automated data ingestion script for Data Career Navigator.
Downloads the latest 'clean_jobs.csv' from Kaggle and saves it to the data/bronze directory.
Intended to be run monthly via GitHub Actions.
"""

import os
import subprocess

def download_kaggle_csv(
    dataset="joykimaiyo18/linkedin-data-jobs-dataset",
    filename="clean_jobs.csv",
    dest_folder="../data/bronze"
):
    """
    Downloads a CSV file from a Kaggle dataset using the Kaggle API.

    Args:
        dataset (str): Kaggle dataset identifier.
        filename (str): Name of the file to download.
        dest_folder (str): Directory to save the downloaded file.
    """
    os.makedirs(dest_folder, exist_ok=True)
    # Download using Kaggle CLI
    cmd = [
        "kaggle", "datasets", "download", dataset,
        "-f", filename, "--unzip", "-p", dest_folder
    ]
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print(f"Downloaded {filename} to {dest_folder}")

if __name__ == "__main__":
    download_kaggle_csv()