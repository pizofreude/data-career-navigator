"""
File: src/scrape_header_text_selenium.py 
-----------------------------------------
This script uses Selenium to scrape job header information from LinkedIn job postings.
It extracts salary, work type, and employment type details from the job page.
"""

# Import necessary libraries
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd

def scrape_linkedin_header_selenium(driver, url):
    """
    Use Selenium to scrape job header information from LinkedIn job postings.
    It extracts salary, work type, and employment type details from the job page.
    It assumes driver is already logged in and ready to scrape.

    Args:
        driver (webdriver): Logged-in Selenium webdriver instance
        url (str): LinkedIn job posting URL

    Returns:
        str: Extracted header/metadata text (if any) or None
    """
    driver.get(url)
    print(f"Navigated to job URL: {url}. Waiting for content to load...")
    try:
        # Wait for the main job content to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(5)  # Give JS time to render

        # Extract salary/bonus info
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "tvm__text--low-emphasis"))
            )
            salary_blocks = driver.find_elements(By.CLASS_NAME, "tvm__text--low-emphasis")
            salary_texts = []
            for span in salary_blocks:
                try:
                    strong = span.find_element(By.TAG_NAME, "strong")
                    text = strong.text.strip()
                    if text:
                        salary_texts.append(text)
                except Exception:
                    continue
        except Exception:
            salary_texts = []

        # Extract work type (e.g., Hybrid)
        work_type_texts = []
        try:
            for span in driver.find_elements(By.CLASS_NAME, "tvm__text--low-emphasis"):
                try:
                    strong = span.find_element(By.TAG_NAME, "strong")
                    text = strong.text.strip()
                    if text and text.lower() in ["hybrid", "remote", "onsite"]:
                        work_type_texts.append(text)
                except Exception:
                    continue
        except Exception:
            pass

        # Extract employment type (e.g., Full-time) from visually-hidden
        employment_type = None
        try:
            for span in driver.find_elements(By.CLASS_NAME, "visually-hidden"):
                text = span.text.strip()
                if "job type is" in text.lower():
                    # e.g., "Matches your job preferences, job type is Full-time."
                    parts = text.split("job type is")
                    if len(parts) > 1:
                        employment_type = parts[1].replace(".", "").strip()
                        break
        except Exception:
            pass

        # Combine all extracted info
        header_info = []
        if salary_texts:
            header_info.append("; ".join(salary_texts))
        if work_type_texts:
            header_info.append("; ".join(work_type_texts))
        if employment_type:
            header_info.append(employment_type)

        if header_info:
            print("\n[INFO] Extracted header/metadata text:\n")
            print("\n".join(header_info))
            return "\n".join(header_info)
        else:
            print("[WARN] No header/metadata info found.")
            return None
    except Exception as e:
        print(f"[WARN] Failed to scrape {url}: {e}")
        return None





INPUT_CSV = "data/bronze/clean_jobs.csv"
OUTPUT_CSV = "data/bronze/clean_jobs_with_header.csv"

def main():
    """
    Main function to scrape LinkedIn job header information and save it to a CSV file.

    This function sets up a Selenium WebDriver with specified options, requires the user
    to manually log into LinkedIn, and then iterates over job posting URLs from an input
    CSV file. It uses the `scrape_linkedin_header_selenium` function to extract header
    information from each job page and appends the results to a new column in the CSV.

    The extracted data is saved to an output CSV file with an additional column for header
    text. The Selenium driver is closed at the end of the process.

    Raises:
        Exception: If any error occurs during the scraping process for a specific URL.
    """

    chrome_options = Options()
    chrome_options.binary_location = r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe"
    # chrome_options.add_argument("--headless=new")  # For batch, you may want to automate login/cookies
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36")
    service = Service("C:\\tools\\chromedriver-win64\\chromedriver.exe")
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Manual login step
    driver.get("https://www.linkedin.com/login")
    print("Please log in to LinkedIn in the opened browser window. After you see your feed or profile, press Enter here to continue...")
    input()

    # Read input CSV
    df = pd.read_csv(INPUT_CSV)

    # Try to read existing output CSV (with header_text), if it exists
    try:
        df_existing = pd.read_csv(OUTPUT_CSV)
        print(f"Loaded existing {OUTPUT_CSV} with {len(df_existing)} rows.")
    except FileNotFoundError:
        df_existing = pd.DataFrame()
        print(f"No existing {OUTPUT_CSV} found. Will create a new one.")

    # Identify new rows to scrape (by 'link')
    if not df_existing.empty and 'link' in df_existing.columns:
        existing_links = set(df_existing['link'].dropna().astype(str))
    else:
        existing_links = set()

    # Only scrape rows whose 'link' is not already in the output file
    mask_new = ~df['link'].astype(str).isin(existing_links)
    df_new = df[mask_new].copy()
    print(f"{len(df_new)} new job links to scrape out of {len(df)} total.")

    header_texts = []
    for scrape_idx, (row_idx, row) in enumerate(df_new.iterrows(), 1):
        url = row.get('link')
        if not isinstance(url, str) or not url.startswith('http'):
            header_texts.append(None)
            continue
        print(f"[{scrape_idx}/{len(df_new)}] Scraping: {url}")
        try:
            header = scrape_linkedin_header_selenium(driver, url)
        except Exception as e:
            print(f"[WARN] Error scraping {url}: {e}")
            header = None
        header_texts.append(header)
        # Optional: sleep to avoid rate-limiting
        time.sleep(2)
    df_new['header_text'] = header_texts

    # Combine with existing (if any), and save
    if not df_existing.empty:
        # Only keep columns present in both, to avoid column mismatch
        common_cols = [col for col in df_existing.columns if col in df_new.columns] + ['header_text']
        df_combined = pd.concat([
            df_existing,
            df_new[[col for col in df_new.columns if col in common_cols]]
        ], ignore_index=True)
        # Deduplicate by 'link'
        df_combined = df_combined.drop_duplicates(subset=['link'])
    else:
        df_combined = df_new

    df_combined.to_csv(OUTPUT_CSV, index=False)
    print(f"Done! Saved with header_text to {OUTPUT_CSV}")
    driver.quit()


# Run the main function if this script is executed directly
if __name__ == "__main__":
    main()