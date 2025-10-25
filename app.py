import asyncio
import aiohttp
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

countries = ["Worldwide"]
excluded_countries = [
    "United States", "USA", "India", "Pakistan", "Philippines", "Israel"
]

keywords = [
    "AI", "IA", "ai automation", "prompt engineer", "ai workflow",
    "foreigner", "foreign", "relocation", "sponsorship", "work permit", "abroad",
    "no code", "low code", "no-code", "low-code",
    "HR AND AI", "HR AND Data", "Marketing", "Social Media", "GEO"
]

filter_keywords = ["zapier", "make.com", "n8n", "Integromat", "Data", "python", "GEO"]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

# --- Async Functions ---
async def fetch(session, url):
    try:
        async with session.get(url, headers=headers, timeout=15) as response:
            return await response.text()
    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")
        return ""

async def fetch_job_links(session, keyword, country):
    links = []
    for i in range(0, 1000):  # max pages
        url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={country}&f_TPR=r86400&start={i*25}"
        html = await fetch(session, url)
        if not html.strip():
            # retry once if empty
            await asyncio.sleep(1)
            html = await fetch(session, url)
            if not html.strip():
                break
        soup = BeautifulSoup(html, "html.parser")
        job_links = soup.find_all("a", class_="base-card__full-link")
        if not job_links:
            break
        for job in job_links:
            job_url = job.get("href")
            if job_url:
                links.append((job_url, keyword))
        await asyncio.sleep(0.2)  # small pause per page
    return links

async def fetch_job_details(session, link, searched_keyword):
    html = await fetch(session, link)
    soup = BeautifulSoup(html, "html.parser")
    
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

        print(f"Total unique links collected: {len(unique_links)}")

        # Step 2: fetch job details concurrently in batches
        jobs = []
        batch_size = 50
        for i in range(0, len(unique_links), batch_size):
            batch = unique_links[i:i+batch_size]
            tasks = [fetch_job_details(session, link, kw) for link, kw in batch]
            results = await asyncio.gather(*tasks)
            jobs.extend([j for j in results if j])
            await asyncio.sleep(1)  # small pause between batches

        df = pd.DataFrame(jobs)
        filtered_df = df[df['Found Filter Keywords'] != ""]
        print(f"\n🎯 Filtered jobs containing AI tools: {len(filtered_df)} / {len(df)}")

        # Step 3: Update Google Sheets
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
