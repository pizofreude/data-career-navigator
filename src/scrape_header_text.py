# src/scrape_header_text.py
"""
File: src/scrape_header_text.py
---------------------------------
Script to scrape the header/metadata text from LinkedIn job postings using the 'link' column in clean_jobs.csv.
Saves the result as a new column 'header_text' in clean_jobs_with_header.csv.

WARNING: LinkedIn may block automated scraping. For robust/production use, consider Selenium/Playwright with login and delays.
This script is for demonstration and educational purposes only.
"""
# Import necessary libraries
import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup

INPUT_CSV = "data/bronze/clean_jobs.csv"
OUTPUT_CSV = "data/bronze/clean_jobs_with_header.csv"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"


def scrape_linkedin_header(url):
    """
    Scrape the header/metadata text from a LinkedIn job posting URL.
    Returns a string with the header text, or None if not found.
    """
    try:
        headers = {"User-Agent": USER_AGENT}
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        # Try to find the header/metadata section
        # LinkedIn job pages often have a <div> or <section> with salary, work type, etc. above the description
        # This selector may need adjustment if LinkedIn changes their layout
        header_texts = []
        # Try to get the first visible block above the job description
        # Look for salary, work type, etc. in the first few <section> or <div> tags
        for tag in soup.find_all(['section', 'div'], limit=10):
            text = tag.get_text(separator=' ', strip=True)
            if any(kw in text.lower() for kw in ["salary", "/yr", "+ bonus", "full-time", "part-time", "contract", "hybrid", "remote", "onsite", "profit sharing"]):
                header_texts.append(text)
        # Fallback: get the first non-empty text block
        if not header_texts:
            for tag in soup.find_all(['section', 'div'], limit=10):
                text = tag.get_text(separator=' ', strip=True)
                if text and len(text) < 200:
                    header_texts.append(text)
        if header_texts:
            # Return the longest header text (most info)
            return max(header_texts, key=len)
        return None
    except Exception as e:
        print(f"[WARN] Failed to scrape {url}: {e}")
        return None

def main():
    """
    Main function to scrape header/metadata text from LinkedIn job postings.
    
    1. Read the input CSV.
    2. Iterate over each row, scrape the header text using scrape_linkedin_header.
    3. Add the scraped header text to the data frame.
    4. Save the updated data frame to a new CSV.
    """
    df = pd.read_csv(INPUT_CSV)
    if 'link' not in df.columns:
        print("[ERROR] No 'link' column in input CSV.")
        return
    header_texts = []
    for idx, row in df.iterrows():
        url = row['link']
        print(f"Scraping header for row {idx}...")
        header = scrape_linkedin_header(url)
        header_texts.append(header)
        # Be polite: random sleep to avoid hammering LinkedIn
        time.sleep(random.uniform(2, 5))
    df['header_text'] = header_texts
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Done! Saved with header_text to {OUTPUT_CSV}")

# Run the main function if this script is executed directly
if __name__ == "__main__":
    main()
