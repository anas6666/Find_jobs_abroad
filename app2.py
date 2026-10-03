import os
import time
import json
import urllib.parse
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from google.oauth2.service_account import Credentials
import gspread

# ---------------------------------------------------------
# Screenshot Directory Setup
# ---------------------------------------------------------
SCREENSHOT_DIR = "screenshots"
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

def capture_screenshot(driver, country_name, page_num):
    """Saves a screenshot when job cards are not found or an error occurs."""
    safe_country = country_name.lower().replace(" ", "_")
    filename = f"{SCREENSHOT_DIR}/no_cards_{safe_country}_page_{page_num}.png"
    try:
        driver.save_screenshot(filename)
        print(f"📸 Screenshot saved: {filename}")
    except Exception as e:
        print(f"⚠️ Failed to take screenshot: {e}")

# ---------------------------------------------------------
# 1. Multi-Country & Domain Setup
# ---------------------------------------------------------
country_map = [
    {"country": "New Zealand", "ext": "nz", "location": "Auckland"},
    {"country": "Australia", "ext": "au", "location": "Australia"},
]

# ---------------------------------------------------------
# 2. Helper Functions for Direct API Fetching
# ---------------------------------------------------------
def fetch_job_details_api(job_key, ext, location_query=""):
    """Fetches full job data using Indeed's fast internal API endpoint."""
    api_url = f"https://{ext}.indeed.com/viewjob?jk={job_key}&spa=1"

    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "referer": "https://ae.indeed.com/jobs?q=&l=Dubai&from=searchOnHP&vjk=beb1873b783ca0a1",
        "sec-ch-ua": '"Not=A?Brand";v="99", "Microsoft Edge";v="151", "Chromium";v="151"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0"
    }

    cookies = {
        "DD_VERSION": "jobseeker-frontend:01acbbd760c598b55e36fe9549084186e0bba5af",
        "CTK": "1k40mh9jahmno800",
        "CSRF": "YPLv98WerD9Cvpqe4RSC4afpsqubGem3",
        "INDEED_CSRF_TOKEN": "pJDWZyt5dHhmaSzkPeIgSyk9s3xSFfz7"
    }

    try:
        res = requests.get(api_url, headers=headers, cookies=cookies, timeout=10)
        if res.status_code == 200:
            data = res.json()
            job_info = data.get("jobModel", {}).get("jobData", {})
            return {
                "Title": job_info.get("jobTitle", "N/A"),
                "Company": job_info.get("companyName", "N/A"),
                "Location": job_info.get("location", {}).get("formattedLocation", "N/A"),
                "Description": job_info.get("sanitizedJobDescription", "N/A")
            }
    except Exception:
        pass
    
    return None

def extract_jk_from_url(url):
    """Extracts job key (jk) parameter from an Indeed job URL."""
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)
    if "jk" in params:
        return params["jk"][0]
    elif "/rc/clk" in parsed.path or "/pagead/clk" in parsed.path:
        if "jk" in params:
            return params["jk"][0]
    return None

# ---------------------------------------------------------
# 3. Selenium Setup
# ---------------------------------------------------------
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--window-size=1920,1080")
options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36")

driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=options)

job_data = []

# ---------------------------------------------------------
# 4. Multi-Country Scraping Loop
# ---------------------------------------------------------
for item in country_map:
    country = item["country"]
    ext = item["ext"]
    location = item["location"]

    print(f"\n🌍 Scraping jobs for {country} ({ext}.indeed.com)")
    job_links = []

    for page in range(0, 2):  # Scrape first 2 pages
        search_url = f'https://{ext}.indeed.com/jobs?q=&l={urllib.parse.quote(location)}&radius=25&fromage=1&start={page * 10}'
        print(f"📄 Page {page + 1}: {search_url}")
        
        try:
            driver.get(search_url)
            time.sleep(2)

            job_cards = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.tapItem, a[id^='job_']"))
            )
            
            for card in job_cards:
                link = card.get_attribute("href")
                if link and link not in job_links:
                    job_links.append(link)

        except TimeoutException:
            print(f"⚠️ No job cards found for {country} on page {page + 1}")
            capture_screenshot(driver, country, page + 1)
            continue
        except Exception as e:
            print(f"⚠️ Error loading page: {e}")
            capture_screenshot(driver, country, page + 1)
            continue

    print(f"✅ Found {len(job_links)} job links in {country}")

    # Process job links using direct requests endpoint
    for i, link in enumerate(job_links, start=1):
        jk = extract_jk_from_url(link)
        
        details = fetch_job_details_api(jk, ext, location) if jk else None
        
        if not details:
            try:
                driver.get(link)
                time.sleep(1)
                
                title = driver.find_element(By.TAG_NAME, "h1").text.strip() if driver.find_elements(By.TAG_NAME, "h1") else "N/A"
                company = driver.find_element(By.CSS_SELECTOR, 'div[data-company-name="true"] a').text.strip() if driver.find_elements(By.CSS_SELECTOR, 'div[data-company-name="true"] a') else "N/A"
                loc = driver.find_element(By.CSS_SELECTOR, 'div[data-testid="inlineHeader-companyLocation"] div').text.strip() if driver.find_elements(By.CSS_SELECTOR, 'div[data-testid="inlineHeader-companyLocation"] div') else "N/A"
                desc = driver.find_element(By.ID, "jobDescriptionText").text.strip() if driver.find_elements(By.ID, "jobDescriptionText") else "N/A"
                
                details = {"Title": title, "Company": company, "Location": loc, "Description": desc}
            except Exception:
                continue

        if details:
            job_data.append({
                "Country": country,
                "Title": details["Title"],
                "Company": details["Company"],
                "Location": details["Location"],
                "Description": details["Description"],
                "Link": link
            })
            print(f"  ({i}/{len(job_links)}) 🏢 {details['Company']} | 💼 {details['Title']}")

driver.quit()

# ---------------------------------------------------------
# 5. Data Post-Processing & Filtering
# ---------------------------------------------------------
df = pd.DataFrame(job_data)

if not df.empty:
    df = df.drop_duplicates(subset=['Link']).reset_index(drop=True)
    df['Description'] = df['Description'].fillna('N/A')

    keywords = ['n8n', 'Zapier', 'make.com', 'Integromat', 'data', 'GEO']

    def get_matching_keywords(desc, target_keywords):
        desc_lower = str(desc).lower()
        matches = [kw for kw in target_keywords if kw.lower() in desc_lower]
        return ', '.join(matches) if matches else None

    df['Matched_Keywords'] = df['Description'].apply(lambda x: get_matching_keywords(x, keywords))
    df = df[df['Matched_Keywords'].notnull()].reset_index(drop=True)
    df = df.drop(columns=['Description'], errors='ignore')

# ---------------------------------------------------------
# 6. Upload to Google Sheets
# ---------------------------------------------------------
if not df.empty:
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
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"\n🎉 Successfully updated Google Sheet with {len(df)} jobs across countries!")
else:
    print("\nℹ️ No matching jobs found across specified countries.")
