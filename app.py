import asyncio
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import json  
import os



# --- Setup ---
yesterday = datetime.now() - timedelta(days=1)
date_str = yesterday.strftime('%Y-%m-%d')

countries = ["EMEA", "East Asia", "Canada"]
excluded_countries = [
    "United States", "USA", "India", "Pakistan", "Philippines", "Israel",
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
    "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", 
    "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", 
    "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", 
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", 
    "New Hampshire", "New Jersey", "New Mexico", "New York", 
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", 
    "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota", 
    "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington", 
    "West Virginia", "Wisconsin", "Wyoming", "Puerto Rico", "Guam", 
    "American Samoa", "Northern Mariana Islands", "U.S. Virgin Islands", 
    "District of Columbia", "Washington D.C.", "DC"
]

keywords = [
    "AI", "IA", "ai automation", "prompt engineer", "ai workflow",
    "foreigner", "foreign", "relocation", "sponsorship", "work permit", "abroad",
    "no code", "low code", "no-code", "low-code",
    "HR AND AI", "HR AND Data", "Marketing", "Social Media", "GEO"
]

filter_keywords = ["zapier", "make.com", "n8n", "Integromat", "Data", "python", "GEO"]

headers = {"User-Agent": "Mozilla/5.0"}

# --- Async Fetch Functions ---
async def fetch(session: ClientSession, url: str):
    try:
        async with session.get(url, headers=headers) as response:
            return await response.text()
    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")
        return ""

async def fetch_job_links(session: ClientSession, keyword: str, country: str):
    links = []
    for i in range(0, 1000):
        url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={country}&f_TPR=r86400&start={i*25}"
        html = await fetch(session, url)
        if not html.strip():
            break
        soup = BeautifulSoup(html, "lxml")
        job_links = soup.find_all("a", class_="base-card__full-link")
        if not job_links:
            break
        for job in job_links:
            job_url = job.get("href")
            if job_url:
                links.append((job_url, keyword))
    return links

async def fetch_job_details(session: ClientSession, link: str, searched_keyword: str):
    html = await fetch(session, link)
    soup = BeautifulSoup(html, "lxml")
    
    title_tag = soup.find(['h1', 'h2'], class_='top-card-layout__title')
    title = title_tag.text.strip() if title_tag else "Not Found"

    company_tag = soup.find('a', class_='topcard__org-name-link')
    company = company_tag.text.strip() if company_tag else "Not Found"

    country_tag = soup.find('span', class_='topcard__flavor--bullet')
    country = country_tag.text.strip() if country_tag else "Not Found"

    desc_tag = soup.find('div', class_='description__text--rich')
    desc = desc_tag.text.strip() if desc_tag else "Not Found"

    if any(excluded.lower() in country.lower() for excluded in excluded_countries):
        return None

    found_keywords = [k for k in filter_keywords if re.search(rf'\b{k}\b', desc, flags=re.IGNORECASE)]
    found_keywords_str = ", ".join(found_keywords) if found_keywords else ""

    return {
        "Date": date_str,
        "Title": title,
        "Company": company,
        "Country": country,
        "Link": link,
        "Searched Keyword": searched_keyword,
        "Found Filter Keywords": found_keywords_str
    }

# --- Main Async Runner ---
async def main():
    all_links = []
    async with aiohttp.ClientSession() as session:
        # Step 1: fetch job links concurrently
        tasks = [fetch_job_links(session, k, c) for k in keywords for c in countries]
        results = await asyncio.gather(*tasks)
        for res in results:
            all_links.extend(res)

        # Remove duplicates
        seen = set()
        unique_links = []
        for link, keyword in all_links:
            if link not in seen:
                unique_links.append((link, keyword))
                seen.add(link)

        # Step 2: fetch job details concurrently
        tasks = [fetch_job_details(session, link, kw) for link, kw in unique_links]
        jobs = await asyncio.gather(*tasks)
        jobs = [j for j in jobs if j]

        # Step 3: create DataFrame
        df = pd.DataFrame(jobs)
        filtered_df = df[df['Found Filter Keywords'] != ""]
        print(f"\n🎯 Filtered jobs containing AI tools: {len(filtered_df)} / {len(df)}")

        # Step 4: update Google Sheets
        service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        client = gspread.authorize(credentials)
        sheet = client.open("LISTE OF JOBS").sheet1
        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print("\n✅ Data successfully updated in Google Sheets!")

# --- Run ---
asyncio.run(main())
