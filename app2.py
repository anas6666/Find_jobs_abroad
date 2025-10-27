import os
import time
import json
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from google.oauth2.service_account import Credentials
import gspread

# ------------------------
# Selenium setup
# ------------------------
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")  # newer headless mode
options.add_argument("--no-sandbox")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=options)

# ------------------------
# Cities and domains
# ------------------------
cities = ["Auckland", "Australia", "Austria","Bahrain","Canada","Czech Republic","Denmark",
          "Finland","Hungary","Italy","Kuwait","Luxembourg","Norway","Poland","Oman",
          "Portugal","Qatar","Saudi Arabia","Singapore","South Korea","España","Sweden",
          "Switzerland","Turkey","UAE","Romania","Jakarta"]
exts = ["nz", "au","at","bh","ca","cz","dk","fi","hu","it","kw","lu","no","pl","om","pt",
        "qt","sa","sg","kr","es","se","ch","tr","ae","ro", "id"]
city_ext_map = dict(zip(cities, exts))

job_data = []

# ------------------------
# Scraping loop
# ------------------------
for city, ext in city_ext_map.items():
    print(f"\n🌆 Scraping jobs for {city} ({ext}.indeed.com)")
    job_links = []

    try:
        for page in range(0, 10):  # only first page
            url = f'https://{ext}.indeed.com/jobs?q=&l={city}&radius=25&fromage=1&from=searchOnDesktopSerp&start={page * 10}'
            print(f"🌍 Page {page+1}: {url}")
            driver.get(url)
            time.sleep(2)

            try:
                job_cards = WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.tapItem"))
                )
            except TimeoutException:
                print(f"⚠️ No job cards found for {city} on page {page+1}")
                continue  # skip to next page/city

            for card in job_cards:
                try:
                    link = card.get_attribute("href")
                    if link and link not in job_links:
                        job_links.append(link)
                except:
                    continue

            print(f"✅ Collected {len(job_links)} links for {city}")

        # Visit each job link
        if not job_links:
            print(f"ℹ️ No job links to visit for {city}")
            continue

        print(f"\n🔎 Visiting {len(job_links)} job pages for {city}")
        for i, link in enumerate(job_links, start=1):
            print(f"({i}/{len(job_links)}) Visiting {link}")
            driver.get(link)
            time.sleep(1)

            try:
                WebDriverWait(driver, 5).until(
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

            except TimeoutException:
                print(f"❌ Could not extract job details for {link}")
            time.sleep(1.5)

    except Exception as e:
        print(f"⚠️ Error scraping {city}: {e}")
        continue  # skip to next city

# ------------------------
# Wrap up
# ------------------------
# ------------------------
# Wrap up
# ------------------------
driver.quit()
df = pd.DataFrame(job_data).drop_duplicates(subset=['Link']).reset_index(drop=True)

# Ensure 'Description' column exists and fill missing with 'N/A'
if 'Description' not in df.columns:
    df['Description'] = 'N/A'
else:
    df['Description'] = df['Description'].fillna('N/A')

# ------------------------
# Keyword filtering
# ------------------------
keywords = ['n8n', 'Zapier', 'make.com', 'Integromat', 'data', 'GEO']

def get_matching_keywords(desc, keywords):
    desc_lower = str(desc).lower()
    matches = [kw for kw in keywords if kw.lower() in desc_lower]
    return ', '.join(matches) if matches else None

df['Matched_Keywords'] = df['Description'].apply(lambda x: get_matching_keywords(x, keywords))
df = df[df['Matched_Keywords'].notnull()].reset_index(drop=True)
df = df.drop(columns=['Description'], errors='ignore')

# ------------------------
# Google Sheets upload
# ------------------------
service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
client = gspread.authorize(credentials)

SPREADSHEET_URL = os.environ["SPREADSHEET_URL"]
WORKSHEET_NAME = 'Indeed Worldwide'

try:
    sheet = client.open_by_url(SPREADSHEET_URL).worksheet(WORKSHEET_NAME)
except gspread.WorksheetNotFound:
    sheet = client.open_by_url(SPREADSHEET_URL).add_worksheet(title=WORKSHEET_NAME, rows="1000", cols="20")

sheet.clear()
if not df.empty:
    sheet.update([df.columns.values.tolist()] + df.values.tolist())

print(f"\n✅ Google Sheet updated with {len(df)} jobs!")

