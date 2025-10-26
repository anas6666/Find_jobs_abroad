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
import json
import os


yesterday = datetime.now() - timedelta(days=1)
date_str = yesterday.strftime('%Y-%m-%d') # e.g. '2025-10-18'

# Step 0 — Setup
countries = ["Worldwide"]
excluded_countries = ["United States", "USA", "États-Unis", "India", "Pakistan","Philippines","Israel"]

keywords = [
    "AI", "IA", "ai automation", "prompt engineer", "ai workflow",
    "foreigner", "foreign", "relocation", "sponsorship", "work permit", "abroad",
    "no code", "low code", "no-code", "low-code",
    "HR AND AI", "HR AND Data", "Marketing", "Social Media", "GEO"
]

# Keywords to detect inside descriptions
filter_keywords = ["zapier", "make.com", "n8n", "Integromat", "Data", "python"]

# Step 1 — Scrape job links
links = []
api_url_job = []

for country in countries:
    for keyword in keywords:  # ✅ search each keyword separately
        for i in range(0, 2):  # Increase range for more pages
            url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={country}&f_TPR=r86400&start={i*25}"
            headers = {"User-Agent": "Mozilla/5.0"}
            
            time.sleep(1)
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, "html.parser")

            job_links = soup.find_all("a", class_="base-card__full-link")

            for job in job_links:
                job_url = job.get("href")
                if job_url and job_url not in links:
                    links.append((job_url, keyword))  # ✅ Store URL + searched keyword
                    match = re.search(r'-([0-9]+)\?', job_url)
                    if match:
                        job_id = match.group(1)
                        api_link = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
                        api_url_job.append(api_link)

print(f"Total job links found: {len(links)}")

# Step 2 — Scrape job details
data = []
headers = {"User-Agent": "Mozilla/5.0"}

for link, searched_keyword in links:
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

        # ✅ Find which filter keywords appear in the description
        found_keywords = [k for k in filter_keywords if re.search(rf'\b{k}\b', desc, flags=re.IGNORECASE)]
        found_keywords_str = ", ".join(found_keywords) if found_keywords else ""

        data.append({
            "Date" : date_str,
            "title": title,
            "company": company,
            "country": country,
            "link": link,
            "searched_keyword": searched_keyword,  
            "found_filter_keywords": found_keywords_str  
        })

    except Exception as e:
        print(f"Error scraping {link}: {e}")

# Step 3 — Create DataFrame
df = pd.DataFrame(data)

# Step 4 — Optional filtering: keep only jobs containing filter keywords
filtered_df = df[df['found_filter_keywords'] != ""]

print(f"Filtered jobs: {len(filtered_df)}")

# Show result


# Step 3: Update Google Sheets

service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
client = gspread.authorize(credentials)

SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1hF8Q-yPVCdoyOHzDqeFn5kBjCLk9AgvOH-mSDOtyNDI/edit'
WORKSHEET_NAME = 'Linkedin Worldwide'

try:
    sheet = client.open_by_url(SPREADSHEET_URL).worksheet(WORKSHEET_NAME)
except gspread.WorksheetNotFound:
    sheet = client.open_by_url(SPREADSHEET_URL).add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="20")

sheet.clear()
sheet.update([df.columns.values.tolist()] + df.values.tolist())

print("\n✅ Data successfully updated in Google Sheets!")


