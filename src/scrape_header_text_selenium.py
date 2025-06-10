from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def scrape_linkedin_header_selenium(driver, url):
    # Assumes driver is already logged in and ready
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


import pandas as pd

INPUT_CSV = "data/bronze/clean_jobs.csv"
OUTPUT_CSV = "data/bronze/clean_jobs_with_header.csv"

def main():
    # Set up Selenium driver and login ONCE
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
    header_texts = []
    for idx, row in df.iterrows():
        url = row.get('link')
        if not isinstance(url, str) or not url.startswith('http'):
            header_texts.append(None)
            continue
        print(f"[{idx+1}/{len(df)}] Scraping: {url}")
        try:
            header = scrape_linkedin_header_selenium(driver, url)
        except Exception as e:
            print(f"[WARN] Error scraping {url}: {e}")
            header = None
        header_texts.append(header)
        # Optional: sleep to avoid rate-limiting
        time.sleep(2)
    df['header_text'] = header_texts
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Done! Saved with header_text to {OUTPUT_CSV}")
    driver.quit()

if __name__ == "__main__":
    main()