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
    "Hong Kong SAR", "Singapore", "Australia", "Canada", "United States", "Spain", "France", "Germany", "United Kingdom", "Ireland",
    "Portugal", "Czech Republic", "Lithuania", "Luxembourg", "Switzerland", "Denmark", "Finland", "Sweden", "Norway"
]

excluded_countries = ["United States", "USA", "États-Unis", "India", "Pakistan", "Philippines", "Israel", "Vietnam"]

# Optional: Add keywords if you want to search by specific terms, or leave empty for all jobs
keywords_for_scraping = [""]  

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
        
    for keyword in keywords_for_scraping:
        if break_step1:
            break
            
        for i in range(0, 20):  # Iterate through pages
            
            # --- Safetime Check ---
            if has_time_expired():
                print("⚠️ Approaching 5.5 hours limit during Step 1! Breaking out early.")
                break_step1 = True
                break

            url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={country}&f_TPR=r86400&start={i*25}"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

            time.sleep(1)
            try:
                response = requests.get(url, headers=headers)
                soup = BeautifulSoup(response.text, "html.parser")
                job_links = soup.find_all("a", class_="base-card__full-link")

                # Break out of page loop if no jobs are returned on this page
                if not job_links:
                    break

                for job in job_links:
                    job_url = job.get("href")
                    if job_url and job_url not in links: # Check if URL is already present
                        links.append(job_url)
                        
                        match = re.search(r'-([0-9]+)\?', job_url)
                        if match:
                            job_id = match.group(1)
                            api_link = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
                            api_url_job.append(api_link)
                            
            except Exception as e:
                print(f"Error fetching search page for {country}: {e}")

print(f"Total unique job links found: {len(links)}")


# ==========================================
# --- STEP 2 — SCRAPE JOB DETAILS ---
# ==========================================
all_job_data = [] # Stores all scraped job details before filtering
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

print("🚀 Starting Step 2: Scraping specific job profiles...")
for link in links:
    
    # --- Safetime Check ---
    if has_time_expired():
        print(f"⚠️ Reached the 5.5 hours benchmark during Step 2. Processing existing ({len(all_job_data)}) records.")
        break

    try:
        time.sleep(1)
        response = requests.get(link, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        title_tag = soup.find('h1', class_='top-card-layout__title') or soup.find('h2', class_='top-card-layout__title')
        title = title_tag.text.strip() if title_tag else "N/A"

        company_tag = soup.find('a', class_='topcard__org-name-link')
        company = company_tag.text.strip() if company_tag else "N/A"

        country_tag = soup.find('span', class_='topcard__flavor--bullet')
        country = country_tag.text.strip() if country_tag else "N/A"

        # 1. Profile Name
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
            raw_url = url_tag.get('href', 'N/A') if url_tag else "N/A"
            
            # Check if it's a genuine LinkedIn profile URL
            if raw_url != "N/A" and "/in/" in raw_url:
                profil_url = raw_url.split('?')[0]
            else:
                profil_url = "N/A"
        except Exception:
            profil_url = "N/A"

        # 4. Job Description Extraction
        try:
            desc_tag = soup.find('div', class_='show-more-less-html__markup') or soup.find('div', class_='description__text')
            job_description = desc_tag.text.strip() if desc_tag else "N/A"
        except Exception:
            job_description = "N/A"

        # 5. Email Search & Extraction via Regex
        email_pattern = r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
        found_emails = re.findall(email_pattern, job_description)
        email_address = found_emails[0] if found_emails else "N/A"

        # Skip excluded countries
        if any(excluded.lower() in country.lower() for excluded in excluded_countries):
            continue

        all_job_data.append({
            "Date": today_date_str,
            "title": title,
            "company": company,
            "country": country,
            "link": link,
            "profil_name": profil_name,
            "profil_tag": profil_tag,
            "profil_url": profil_url,
            "job_description": job_description,
            "email": email_address
        })

    except Exception as e:
        print(f"Error scraping details for {link}: {e}")


# ==========================================
# --- STEP 3 TO 5 — PROCESS & SAVE TO GOOGLE SHEETS ---
# ==========================================
if not all_job_data:
    print("❌ No data was parsed during this execution window. Google Sheets will remain unchanged.")
else:
    # --- Step 3 — Create DataFrame & Filter Data ---
    df_jobs = pd.DataFrame(all_job_data)
    
    # 1. Remove any row that contains "N/A" in core columns (ignoring email/description N/A so rows aren't wiped out)
    core_columns = ["Date", "title", "company", "country", "link", "profil_name", "profil_tag", "profil_url"]
    df_jobs = df_jobs[~df_jobs[core_columns].eq("N/A").any(axis=1)].reset_index(drop=True)
    
    # 2. Local deduplication based on job link
    df_jobs = df_jobs.drop_duplicates(subset=['link']).reset_index(drop=True)
    
    # 3. Filter rows where 'profil_tag' contains specific keywords (case-insensitive)
    filter_keywords = ["senior", "lead", "director", "direcotr", "founder", "co-founder","Managing","Partner"]
    pattern = '|'.join(filter_keywords)
    df_jobs = df_jobs[df_jobs['profil_tag'].str.contains(pattern, case=False, na=False)].reset_index(drop=True)
    
    # 4. Reorder columns to match your Google Sheet layout
    columns_order = [
        "Date", "title", "company", "country", "link", 
        "profil_name", "profil_tag", "profil_url", "job_description", "email"
    ]
    df_jobs = df_jobs[columns_order]
    
    if df_jobs.empty:
        print("⚠️ All scraped rows contained 'N/A', duplicates, or didn't match the target tags. Nothing to upload.")
    else:
        # --- Step 4 — Connect to Google Sheets & Overwrite Data (Row 2 Downward) ---
        try:
            service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
            SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
            client = gspread.authorize(credentials)

            SPREADSHEET_URL = os.environ["SPREADSHEET_URL"]
            spreadsheet = client.open_by_url(SPREADSHEET_URL)

            # Target tab name
            WORKSHEET_NAME = 'Recruiters'

            try:
                worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="10")
                # Write header row if worksheet is newly created
                worksheet.append_row(columns_order, value_input_option='USER_ENTERED')

            print(f"\n🧹 Clearing previous data from '{WORKSHEET_NAME}' (Row 1 headers will remain intact)...")

            # 1. Clear all rows from Row 2 down
            worksheet.batch_clear(['A2:Z10000'])

            print(f"🚀 Writing {len(df_jobs)} fresh records starting at cell A2...")

            # 2. Write the new dataset starting at cell A2
            worksheet.update('A2', df_jobs.values.tolist(), value_input_option='USER_ENTERED')

            print(f"✅ Successfully updated '{WORKSHEET_NAME}' with {len(df_jobs)} fresh records!")

        except KeyError as e:
            print(f"❌ Missing environment variable: {e}")
        except Exception as e:
            print(f"❌ Error updating Google Sheets: {e}")

print(f"🏁 Execution finished gracefully. Total time elapsed: {round((time.time() - START_TIME) / 60, 2)} minutes.")
