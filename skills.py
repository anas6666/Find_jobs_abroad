import time
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime, timedelta
import os
import json


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
    "Morocco","Saudi Arabia", "United Arab Emirates",
    "Japan", "South Korea", "Hong Kong SAR", "Singapore", "Australia","Canada", "Spain", "France", "Germany", "United Kingdom",
    "Portugal", "Czech Republic", "Lithuania", "Luxembourg",
    "Switzerland", "Denmark", "Finland", "Sweden", "Norway", "Ireland","United States"
]

excluded_countries = ["India", "Pakistan", "Philippines", "Israel", "Vietnam"]

keywords_for_scraping = [ 
    "AI", "IA", "ai automation", "prompt", "workflow", "automatisation", "automation",
    "foreigner", "foreign", "relocation", "sponsorship", "work permit", "abroad",
    "no code", "low code", "no-code", "low-code", "nocode", "Data", "RPA", "n8n", "llm",
    "GTM", "Marketing", "Social Media", "GEO", "SEO"
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
        for i in range(0, 3):  
            
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
# --- STEP 3 — SAVE RESULTS TO EXCEL ---
# ==========================================
import os

# Convert list of dictionaries to DataFrame
df = pd.DataFrame(all_job_data)

# Save as Excel directly in the main path
output_file = "linkedin_jobs.xlsx"

df.to_excel(output_file, index=False, engine="openpyxl")

print(f"✅ Saved {len(df)} jobs to {output_file}")
