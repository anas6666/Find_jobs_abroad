%%time

import pandas as pd
import requests
import re
from bs4 import BeautifulSoup as bs
import os 
import glob
import time
import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException


driver = webdriver.Chrome()

cities = ["Auckland", "Australia", "austria","Bahrain","Canada","Czech Republic","denmark","finland","hungary","italy","Kuwait","Luxembourg","norway","poland","oman","portugal","qatar","Saudi Arabia", "Singapore","south korea","España","sweden","switzerland","turkey","uae","romania","Jakarta"]
exts = ["nz", "au","at","bh","ca","cz","dk","fi","hu","it","kw","lu","no","pl","om","pt","qt","sa","sg","kr","es","se", "ch","tr","ae","ro", "id"]  # I changed 'sw' to 'ch' (Switzerland's real Indeed domain)
city_ext_map = dict(zip(cities, exts))

job_data = []

# --- Scraping Loop ---
for city, ext in city_ext_map.items():
    print(f"\n🌆 Starting scrape for {city} ({ext}.indeed.com)")

    job_links = []

    try:
        # STEP 1: Collect job links
        for page in range(0, 1):  
            url = f'https://{ext}.indeed.com/jobs?q=&l={city}&radius=25&fromage=1&from=searchOnDesktopSerp&start={page * 10}'
            print(f"\n🌍 Scraping page {page + 1}: {url}")
            driver.get(url)
            time.sleep(2)

            try:
                job_cards = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.cardOutline.tapItem.dd-privacy-allow.result"))
                )

                for card in job_cards:
                    try:
                        job_link = card.find_element(By.CSS_SELECTOR, "a.jcs-JobTitle").get_attribute("href")
                        if job_link and job_link not in job_links:
                            job_links.append(job_link)
                    except:
                        continue

                print(f"✅ Links collected so far for {city}: {len(job_links)}")

            except Exception as e:
                print(f"⚠️ Error collecting links on page {page + 1}: {e}")

        # STEP 2: Visit each job link
        print(f"\n🔎 Visiting {len(job_links)} job pages for {city}...\n")
        for i, link in enumerate(job_links, start=1):
            print(f"({i}/{len(job_links)}) Visiting: {link}")
            driver.get(link)
            time.sleep(1)

            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "jobDescriptionText"))
                )

                title = driver.find_element(By.TAG_NAME, "h1").text.strip() if driver.find_elements(By.TAG_NAME, "h1") else "N/A"
                company = driver.find_element(By.CSS_SELECTOR, 'div[data-company-name="true"] a').text.strip() if driver.find_elements(By.CSS_SELECTOR, 'div[data-company-name="true"] a') else "N/A"
                location = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="inlineHeader-companyLocation"] div').text.strip() if driver.find_elements(By.CSS_SELECTOR, 'div[data-testid="inlineHeader-companyLocation"] div') else "N/A"
                desc = driver.find_element(By.ID, "jobDescriptionText").text.strip() if driver.find_elements(By.ID, "jobDescriptionText") else "N/A"

                job_data.append({
                    "City": city,
                    "Title": title,
                    "Company": company,
                    "Location": location,
                    "Description": desc,
                    "Link": link
                })

                print(f"🏢 {company} | 📍 {location} | 💼 {title}")

            except Exception as e:
                print(f"❌ Could not extract details: {e}")

            time.sleep(1.5)

    except Exception as e:
        print(f"⚠️ Error while scraping {city}: {e}")


# --- Wrap Up ---
driver.quit()
df = pd.DataFrame(job_data)

print(f"\n✅ Extracted {len(df)} jobs total across all cities.")

df1 = df.drop_duplicates(subset=['Link'], keep='first').reset_index(drop=True)

keywords = ['n8n', 'Zapier', 'make.com', 'Integromat','data','GEO']

# Function to find matching keywords in a description
def get_matching_keywords(desc, keywords):
    desc_lower = str(desc).lower()
    matches = [kw for kw in keywords if kw.lower() in desc_lower]
    return ', '.join(matches) if matches else None

# Add a new column with matched keywords
df1['Matched_Keywords'] = df1['Description'].apply(lambda x: get_matching_keywords(x, keywords))

# Keep only rows that have matches (already filtered previously, but safe)
df1 = df1[df1['Matched_Keywords'].notnull()].reset_index(drop=True)

# Drop the original Description column
df1 = df1.drop(columns=['Description'])

# --- send to google sheets

import gspread
from google.oauth2.service_account import Credentials

# --- Setup ---
SERVICE_ACCOUNT_FILE = 'linkedin.json'  # path to your service account JSON
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

client = gspread.authorize(credentials)

# --- Open spreadsheet by URL ---
SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/1hF8Q-yPVCdoyOHzDqeFn5kBjCLk9AgvOH-mSDOtyNDI/edit'  # replace with your sheet URL
spreadsheet = client.open_by_url(SPREADSHEET_URL)

# --- Select worksheet by name ---
WORKSHEET_NAME = 'Indeed Worldwide'  # replace with your worksheet name
try:
    sheet = spreadsheet.worksheet(WORKSHEET_NAME)
except gspread.WorksheetNotFound:
    # If worksheet doesn't exist, create it
    sheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="20")

# Optional: clear existing data
sheet.clear()

# --- Update sheet with DataFrame ---
# df is your filtered DataFrame
sheet.update([df1.columns.values.tolist()] + df1.values.tolist())

print("Google Sheet updated successfully!")
