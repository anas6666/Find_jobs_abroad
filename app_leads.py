import time
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime, timedelta
import os
import json
import gspread
from google.oauth2.service_account import Credentials

# ==========================================
# --- TIME TRACKING CONFIGURATION (SAFEGUARD) ---
# ==========================================
START_TIME = time.time()
# 5.5 hours = 5.5 * 60 * 60 = 19,800 seconds
MAX_DURATION_SECONDS = 21000 

def has_time_expired():
    """Returns True if the script has been running for more than 5.5 hours."""
    elapsed = time.time() - START_TIME
    return elapsed >= MAX_DURATION_SECONDS

# ==========================================
# --- CONFIGURATION & SEARCH CRITERIA ---
# ==========================================
yesterday = datetime.now() - timedelta(days=1)
today_date_str = datetime.now().strftime('%Y-%m-%d') # Use today's date for counts

countries = [
    "Qatar", "Oman", "Kuwait", "Bahrain", "Dubai", "United Arab Emirates", "Hong Kong SAR", "Singapore","European Economic Area"
]

excluded_countries = ["United States", "USA", "États-Unis", "India", "Pakistan", "Philippines", "Israel", "Vietnam"]

# ==========================================
# --- STEP 1 — SCRAPE JOB LINKS ---
# ==========================================
links = []
api_url_job = []
break_step1 = False

print("🚀 Starting Step 1: Scraping job links...")
for country in countries:
    if break_step1:
        break
        for i in range(0, 20):  # Increase range for more pages
            
            # --- Safetime Check ---
            if has_time_expired():
                print("⚠️ Approaching 5.5 hours limit during Step 1! Breaking out of link collection early to save data.")
                break_step1 = True
                break

            url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?&location={country}&f_TPR=r86400&start={i*25}"
            headers = {"User-Agent": "Mozilla/5.0"}

            time.sleep(1)
            try:
                response = requests.get(url, headers=headers)
                soup = BeautifulSoup(response.text, "html.parser")
                job_links = soup.find_all("a", class_="base-card__full-link")

                for job in job_links:
                    job_url = job.get("href")
                    if job_url and job_url not in [link[0] for link in links]: # Check if URL is already present
                        links.append((job_url, keyword))
                        match = re.search(r'-([0-9]+)\?', job_url)
                        if match:
                            job_id = match.group(1)
                            api_link = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
                            api_url_job.append(api_link)
            except Exception as e:
                print(f"Error fetching search page: {e}")

print(f"Total unique job links found: {len(links)}")


# ==========================================
# --- STEP 2 — SCRAPE JOB DETAILS ---
# ==========================================
all_job_data = [] # Stores all scraped job details before filtering
headers = {"User-Agent": "Mozilla/5.0"}

print("🚀 Starting Step 2: Scraping specific job profiles...")
for link in links:
    
    # --- Safetime Check ---
    if has_time_expired():
        print(f"⚠️ Reached the 5.5 hours benchmark during Step 2. Activating fallback script to process existing ({len(all_job_data)}) records.")
        break

    try:
        time.sleep(1)
        response = requests.get(link, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        title_tag = soup.find('h1', class_='top-card-layout__title') or soup.find('h2', class_='top-card-layout__title')
        title = title_tag.text.strip() if title_tag else "Not Found"

        company_tag = soup.find('a', class_='topcard__org-name-link')
        company = company_tag.text.strip() if company_tag else "Not Found"

        country_tag = soup.find('span', class_='topcard__flavor--bullet')
        country = country_tag.text.strip() if country_tag else "Not Found"

        try:
            name_tag = soup.find('h3', class_='base-main-card__title')
            profil_name = name_tag.text.strip() if name_tag else "N/A"
        except Exception:
            profil_name = "N/A"
        
        # 2. Profile Tag / Headline
        try:
            tag_element = soup.find('h4', class_='base-main-card__subtitle')
            profil_tag = tag_element.text.strip() if tag_element else "N/A"
        except Exception:
            profil_tag = "N/A"
        
        # 3. Profile URL
        try:
            url_tag = soup.find('a', class_='base-card__full-link')
            profil_url = url_tag.get('href', 'N/A') if url_tag else "N/A"
        except Exception:
            profil_url = "N/A"

        # Skip excluded countries
        if any(excluded.lower() in country.lower() for excluded in excluded_countries):
            continue

        all_job_data.append({
            "Date": today_date_str,
            "title": title,
            "company": company,
            "country": country,
            "link": link,
            "profil_name" : profil_name,
            "profil_tag": profil_tag,
            "profil_url": profil_url
        })

    except Exception as e:
        print(f"Error scraping details for {link}: {e}")


# ==========================================
# --- STEP 3 TO 5 — PROCESS & SAVE TO GOOGLE SHEETS ---
# ==========================================
if not all_job_data:
    print("❌ No data was parsed during this execution window. Google Sheets will remain unchanged.")
else:
    # --- Step 3 — Create DataFrame & Deduplicate ---
    df_jobs = pd.DataFrame(all_job_data)
    
    # Deduplicate based on the job link
    df_jobs = df_jobs.drop_duplicates(subset=['link']).reset_index(drop=True)
    
    # Enforce standard column ordering
    columns_order = [
        "Date", "title", "company", "country", "link", 
        "profil_name", "profil_tag", "profil_url"
    ]
    df_jobs = df_jobs[columns_order]
    
    print(f"Total unique jobs ready for upload: {len(df_jobs)}")

    # --- Step 4 — Connect to Google Sheets ---
    try:
        service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        client = gspread.authorize(credentials)

        SPREADSHEET_URL = os.environ["SPREADSHEET_URL"]
        spreadsheet = client.open_by_url(SPREADSHEET_URL)

        # Target worksheet name (change this if you prefer a different sheet name)
        WORKSHEET_NAME_2 = 'Recruiters'

        try:
            worksheet = spreadsheet.worksheet(WORKSHEET_NAME_2)
        except gspread.WorksheetNotFound:
            # Create worksheet if it doesn't exist yet
            worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME_2, rows="1000", cols="10")

        print(f"\nUploading data to '{WORKSHEET_NAME_2}'...")

        # Get existing data to check if headers are already set
        existing_rows = worksheet.get_all_values()

        if not existing_rows:
            # Sheet is empty: Write headers first, then data
            worksheet.append_row(columns_order, value_input_option='USER_ENTERED')
            worksheet.append_rows(df_jobs.values.tolist(), value_input_option='USER_ENTERED')
        else:
            # Append rows directly under existing data
            worksheet.append_rows(df_jobs.values.tolist(), value_input_option='USER_ENTERED')

        print(f"✅ Successfully appended {len(df_jobs)} records to '{WORKSHEET_NAME_2}'!")

    except KeyError as e:
        print(f"❌ Missing environment variable: {e}")
    except Exception as e:
        print(f"❌ Error uploading to Google Sheets: {e}")

print(f"🏁 Execution finished gracefully. Total time elapsed: {round((time.time() - START_TIME) / 60, 2)} minutes.")
