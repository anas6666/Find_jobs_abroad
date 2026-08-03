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
MAX_DURATION_SECONDS = 21000 

def has_time_expired():
    """Returns True if the script has been running for more than 5.5 hours."""
    elapsed = time.time() - START_TIME
    return elapsed >= MAX_DURATION_SECONDS

# ==========================================
# --- CONFIGURATION & SEARCH CRITERIA ---
# ==========================================
yesterday = datetime.now() - timedelta(days=1)
today_date_str = datetime.now().strftime('%Y-%m-%d')

countries = [
    "Morocco", "South Africa",
    "Qatar", "Oman", "Kuwait", "Bahrain", "Saudi Arabia", "United Arab Emirates",
    "Japan", "South Korea", "Hong Kong SAR", "Singapore", "Australia",
    "Turkey", "Canada", "Spain", "France", "Germany", "United Kingdom",
    "Portugal", "Romania", "Czech Republic", "Lithuania", "Hungary", "Cyprus", "Luxembourg",
    "Switzerland", "Estonia", "Denmark", "Finland", "Sweden", "Norway", "austria", "Ireland"
]

excluded_countries = ["United States", "USA", "États-Unis", "India", "Pakistan", "Philippines", "Israel", "Vietnam"]

keywords_for_scraping = [ 
    "AI", "IA", "ai automation", "prompt", "workflow", "automatisation", "automation",
    "foreigner", "foreign", "relocation", "sponsorship", "work permit", "abroad",
    "no code", "low code", "no-code", "low-code", "nocode", "Data", "RPA", "n8n", "llm",
    "GTM", "Marketing", "Social Media", "GEO", "SEO"
]

# Keywords for the "Linkedin Worldwide" sheet filter
linkedin_worldwide_filter_keywords = [
    "n8n", "zapier", "make.com", "integromat", "activepieces"
]

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
        for i in range(0, 2):  
            
            # --- Safetime Check ---
            if has_time_expired():
                print("⚠️ Approaching 5.5 hours limit during Step 1! Breaking out of link collection early to save data.")
                break_step1 = True
                break

            url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=Remote&{keyword}&location={country}&f_TPR=r86400&start={i*25}"
            headers = {"User-Agent": "Mozilla/5.0"}

            time.sleep(1)
            try:
                response = requests.get(url, headers=headers)
                soup = BeautifulSoup(response.text, "html.parser")
                job_links = soup.find_all("a", class_="base-card__full-link")

                for job in job_links:
                    job_url = job.get("href")
                    if job_url and job_url not in [link[0] for link in links]: 
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
all_job_data = [] 
headers = {"User-Agent": "Mozilla/5.0"}

print("🚀 Starting Step 2: Scraping specific job profiles...")
for link, searched_keyword in links:
    
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

        desc_tag = soup.find('div', class_='description__text--rich')
        desc = desc_tag.text.strip() if desc_tag else "Not Found"

        # Skip excluded countries
        if any(excluded.lower() in country.lower() for excluded in excluded_countries):
            continue

        all_job_data.append({
            "Date": today_date_str,
            "title": title,
            "company": company,
            "country": country,
            "link": link,
            "searched_keyword": searched_keyword,
            "description": desc 
        })

    except Exception as e:
        print(f"Error scraping details for {link}: {e}")

# ==========================================
# --- STEP 3 TO 5 — PROCESS & SAVE DATA ---
# ==========================================
if not all_job_data:
    print("❌ No data was parsed during this execution window. Google Sheets will remain unchanged.")
else:
    # --- Step 3 — Create DataFrame from all scraped data ---
    df_all_jobs = pd.DataFrame(all_job_data)
    df_all_jobs = df_all_jobs.drop_duplicates(subset=['link']).reset_index(drop=True)
    print(f"Total unique jobs scraped (after initial deduplication): {len(df_all_jobs)}")

    # --- Step 4 — Process for "Linkedin Worldwide" sheet ---
    def check_worldwide_keywords(description):
        found_world_keywords = [k for k in linkedin_worldwide_filter_keywords if re.search(r'\b' + re.escape(k) + r'\b', description, flags=re.IGNORECASE)]
        return ", ".join(found_world_keywords) if found_world_keywords else ""

    df_all_jobs['found_linkedin_worldwide_keywords'] = df_all_jobs['description'].apply(check_worldwide_keywords)
    filtered_worldwide_df = df_all_jobs[df_all_jobs['found_linkedin_worldwide_keywords'] != ""].copy()

    # Select and reorder columns for "Linkedin Worldwide" sheet
    filtered_worldwide_df = filtered_worldwide_df[[
        "Date", "title", "company", "country", "link", "searched_keyword", "found_linkedin_worldwide_keywords"
    ]].rename(columns={"found_linkedin_worldwide_keywords": "Found Keywords"}) 

    print(f"Jobs for 'Linkedin Worldwide' sheet (unique and filtered): {len(filtered_worldwide_df)}")

    # --- Step 5: Connect and Update Google Sheets ---
    service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    client = gspread.authorize(credentials)

    SPREADSHEET_URL = os.environ["SPREADSHEET_URL"]

    # --- Update "Linkedin Worldwide" Sheet ---
    WORKSHEET_NAME_WORLDWIDE = 'Linkedin Remote'
    try:
        sheet_worldwide = client.open_by_url(SPREADSHEET_URL).worksheet(WORKSHEET_NAME_WORLDWIDE)
    except gspread.WorksheetNotFound:
        sheet_worldwide = client.open_by_url(SPREADSHEET_URL).add_worksheet(title=WORKSHEET_NAME_WORLDWIDE, rows="1000", cols="20")

    print(f"\nUpdating '{WORKSHEET_NAME_WORLDWIDE}' sheet...")
    sheet_worldwide.clear() 
    
    if not filtered_worldwide_df.empty:
        sheet_worldwide.update(
            [filtered_worldwide_df.columns.values.tolist()] +
            filtered_worldwide_df.values.tolist()
        )
    else:
        sheet_worldwide.update([["Date", "title", "company", "country", "link", "searched_keyword", "Found Keywords"]])
    print(f"✅ Data successfully updated in '{WORKSHEET_NAME_WORLDWIDE}'!")

print(f"🏁 Execution finished gracefully. Total time elapsed: {round((time.time() - START_TIME) / 60, 2)} minutes.")
