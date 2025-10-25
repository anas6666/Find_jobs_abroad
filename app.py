%%time

import time
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# --- Step 0 — Setup ---
yesterday = datetime.now() - timedelta(days=1)
date_str = yesterday.strftime('%Y-%m-%d')

countries = ["Worldwide"]
excluded_countries = [
    # Countries
    "United States", "USA", "États-Unis", "India", "Pakistan", "Philippines", "Israel",

    # All U.S. States and Territories
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", 
    "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", 
    "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", 
    "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", 
    "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", 
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", 
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", 
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",

    # U.S. Territories
    "Puerto Rico", "Guam", "American Samoa", "Northern Mariana Islands", "U.S. Virgin Islands", 
    "District of Columbia", "Washington D.C.", "DC"
]

keywords = [
    "AI", "IA", "ai automation", "prompt engineer", "ai workflow",
    "foreigner", "foreign", "relocation", "sponsorship", "work permit", "abroad",
    "no code", "low code", "no-code", "low-code",
    "HR AND AI", "HR AND Data", "Marketing", "Social Media", "GEO"
]

filter_keywords = ["zapier", "make.com", "n8n", "Integromat", "Data", "python"]

# --- Step 1 — Scrape job links ---
links = []
api_url_job = []
headers = {"User-Agent": "Mozilla/5.0"}

for country in countries:
    for keyword in keywords:
        print(f"\n🔍 Searching for '{keyword}' in '{country}'...")
        for i in range(0, 100):  # ✅ Go all the way to page 100
            url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={country}&f_TPR=r86400&start={i*25}"
            time.sleep(1.5)
            
            response = requests.get(url, headers=headers)
            if response.status_code != 200 or not response.text.strip():
                print(f"⚠️ No more results at page {i} for {keyword}.")
                break  # Stop when LinkedIn stops returning jobs
            
            soup = BeautifulSoup(response.text, "html.parser")
            job_links = soup.find_all("a", class_="base-card__full-link")
            if not job_links:
                print(f"⚠️ Page {i} empty for {keyword}. Stopping.")
                break
            
            for job in job_links:
                job_url = job.get("href")
                if job_url and job_url not in [l[0] for l in links]:
                    links.append((job_url, keyword))
                    match = re.search(r'-([0-9]+)\?', job_url)
                    if match:
                        job_id = match.group(1)
                        api_url_job.append(f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}")

print(f"\n✅ Total job links found: {len(links)}")

# --- Step 2 — Scrape job details ---
data = []
for idx, (link, searched_keyword) in enumerate(links):
    try:
        time.sleep(1)
        response = requests.get(link, headers=headers)
        soup = BeautifulSoup(response.text, "html.parser")

        title_tag = soup.find(['h1', 'h2'], class_='top-card-layout__title')
        title = title_tag.text.strip() if title_tag else "Not Found"

        company_tag = soup.find('a', class_='topcard__org-name-link')
        company = company_tag.text.strip() if company_tag else "Not Found"

        country_tag = soup.find('span', class_='topcard__flavor--bullet')
        country = country_tag.text.strip() if country_tag else "Not Found"

        desc_tag = soup.find('div', class_='description__text--rich')
        desc = desc_tag.text.strip() if desc_tag else "Not Found"

        if any(excluded.lower() in country.lower() for excluded in excluded_countries):
            continue

        found_keywords = [k for k in filter_keywords if re.search(rf'\b{k}\b', desc, flags=re.IGNORECASE)]
        found_keywords_str = ", ".join(found_keywords) if found_keywords else ""

        data.append({
            "Date": date_str,
            "Title": title,
            "Company": company,
            "Country": country,
            "Link": link,
            "Searched Keyword": searched_keyword,
            "Found Filter Keywords": found_keywords_str
        })

        if idx % 25 == 0:
            print(f"🧩 Scraped {idx}/{len(links)} jobs...")

    except Exception as e:
        print(f"❌ Error scraping {link}: {e}")

# --- Step 3 — Create and filter DataFrame ---
df = pd.DataFrame(data)
filtered_df = df[df['Found Filter Keywords'] != ""]
print(f"\n🎯 Filtered jobs containing AI tools: {len(filtered_df)} / {len(df)}")

# --- Step 4 — Send to Google Sheets ---
SERVICE_ACCOUNT_FILE = 'linkedin.json'
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]

credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(credentials)
sheet = client.open("LISTE OF JOBS").sheet1

sheet.clear()
sheet.update([df.columns.values.tolist()] + df.values.tolist())

print("\n✅ Data successfully updated in Google Sheets!")
