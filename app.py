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

# --- Configuration ---
yesterday = datetime.now() - timedelta(days=1)
today_date_str = datetime.now().strftime('%Y-%m-%d') # Use today's date for counts

countries = ["Morocco","South Africa","Mauritius","Seychelles",
             "Qatar","Oman","Kuwait","Bahrain","Saudi Arabia","United Arab Emirates","Jordan",
             "Japan", "South Korea","Hong Kong SAR", "Singapore","Australia","New Zealand",
             "Turkey","Bosnia and Herzegovina","Georgia","Albania","Ukraine","Russia","Canada",
            "Portugal", "Malta", "Romania", "Czech Republic", "Latvia", "Lithuania", "Hungary", "Slovakia", "Cyprus","Luxembourg",
             "Iceland","Greenland","Switzerland","Estonia","Denmark","Finland","Sweden","Norway", "European Economic Area"
            ]
excluded_countries = ["United States", "USA", "États-Unis", "India", "Pakistan","Philippines","Israel","Vietnam"]

keywords_for_scraping = [ # Keywords used to search on LinkedIn
    "AI", "IA", "ai automation", "prompt", "workflow","automatisation", "automation",
    "foreigner", "foreign", "relocation", "sponsorship", "work permit", "abroad",
    "no code", "low code", "no-code", "low-code","nocode", "Data", "RPA", "n8n","llm",
    "GTM", "Marketing", "Social Media", "GEO", "SEO"
]

# Keywords for the "Linkedin Worldwide" sheet filter
linkedin_worldwide_filter_keywords = [
    "n8n", "zapier", "make.com", "integromat"
]

# --- Define Skill Categories and Skills within them ---
# This dictionary maps a tag to a list of skills under that tag
skill_categories = {
    "Data Analyst": [
        "VBA","power query","DAX","power bi","tableau",
        "data visualization", "reporting","data analysis","web scraping",
        "looker", "qlik"
    ],
    "Data Engineer": [
        "etl", "airflow", "dbt", "spark", "kafka", "hadoop", "snowflake", "databricks", "redshift", "bigquery",
        "batch processing", "stream processing", "data modeling", "data pipelines"
    ],
    "Data Scientist": [
        "data science", "machine learning", "predictive modeling", "model evaluation",
        "statistics", "nlp", "computer vision","ML", "deep learning", "dl",
        "scikit-learn", "tensorflow", "pytorch", "keras", "xgboost", "lightgbm",
        "time series", "a/b testing"
    ],
            
    "AI/ML Engineer": [
        "artificial intelligence", "ai", "llm", "large language models", "prompt",
        "fine-tuning","fine tuning", "rag","generative ai", "genai", "gen ai",
        "retrieval augmented generation", "hugging face", "openai", "gemini",
        "deepseek", "claude", "transformers", "bert", "llama","ollama",
        "vector databases", "mlops"
    ],
            
    "AI Automation/RPA/No-Code": [
        "n8n", "zapier", "make.com", "integromat", "uipath", "power automate",
        "power apps", "mendix", "automation anywhere", "rpa", "appian"
    ],
            
    "Programming Languages": [
        "python", "javascript", "go", "java", "c#", "scala",
        "kotlin", "swift", "php", "ruby", "rust", "HTML", "CSS", "sql","nosql",
        "typescript", "bash", "shell scripting", "R"
    ],
    "DevOps": [
        "docker", "kubernetes", "ci/cd", "github actions", "gitlab ci/cd", "jenkins",
        "terraform", "ansible", "helm", "prometheus", "grafana", "git"
                
    ],
    "Cloud Platforms": [
        "aws", "azure", "gcp"
    ],
            
    "Databases": [
        "postgresql", "mysql", "mongodb", "redis", "cassandra","sqlite"
    ],
            
    "Frontend/UI/UX": [
        "react", "angular", "vue.js", "canva", "figma", "adobe"
    ],
            
    "Backend/API Development": [
        "rest api", "graphql","node.js", "express.js",
        "spring boot", "django", "flask", "fastapi", "asp.net"
    ],
            
    "Project/Product Management": [
        "agile", "scrum", "jira"
    ]
}

# Consolidate all skills into a single list for scraping and create a skill-to-tag map
count_skills_keywords = []
skill_to_tag_map = {}
for tag, skills in skill_categories.items():
    for skill in skills:
        count_skills_keywords.append(skill)
        skill_to_tag_map[skill] = tag # Map each skill to its primary tag

# Ensure unique skills in the consolidated list, though the dictionary structure helps
count_skills_keywords = list(set(count_skills_keywords))

# --- Step 1 — Scrape job links ---
links = []
api_url_job = []

for country in countries:
    for keyword in keywords_for_scraping:
        for i in range(0, 2):  # Increase range for more pages
            url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={country}&f_TPR=r86400&start={i*25}"
            headers = {"User-Agent": "Mozilla/5.0"}

            time.sleep(1)
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.text, "html.parser")

            job_links = soup.find_all("a", class_="base-card__full-link")

            for job in job_links:
                job_url = job.get("href")
                if job_url and job_url not in [link[0] for link in links]: # Check if URL (first element of tuple) is already present
                    links.append((job_url, keyword))
                    match = re.search(r'-([0-9]+)\?', job_url)
                    if match:
                        job_id = match.group(1)
                        api_link = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
                        api_url_job.append(api_link)

print(f"Total unique job links found: {len(links)}")

# --- Step 2 — Scrape job details ---
all_job_data = [] # Stores all scraped job details before filtering
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

        all_job_data.append({
            "Date": today_date_str,
            "title": title,
            "company": company,
            "country": country,
            "link": link,
            "searched_keyword": searched_keyword,
            "description": desc # Keep description for skill counting later
        })

    except Exception as e:
        print(f"Error scraping {link}: {e}")

# --- Step 3 — Create DataFrame from all scraped data ---
df_all_jobs = pd.DataFrame(all_job_data)
df_all_jobs = df_all_jobs.drop_duplicates(subset=['link']).reset_index(drop=True)
print(f"Total unique jobs scraped (after initial deduplication): {len(df_all_jobs)}")

# --- Step 4 — Process for "Linkedin Worldwide" sheet ---
# Filter jobs based on linkedin_worldwide_filter_keywords
def check_worldwide_keywords(description):
    found_world_keywords = [k for k in linkedin_worldwide_filter_keywords if re.search(r'\b' + re.escape(k) + r'\b', description, flags=re.IGNORECASE)]
    return ", ".join(found_world_keywords) if found_world_keywords else ""

df_all_jobs['found_linkedin_worldwide_keywords'] = df_all_jobs['description'].apply(check_worldwide_keywords)
filtered_worldwide_df = df_all_jobs[df_all_jobs['found_linkedin_worldwide_keywords'] != ""].copy()

# Select and reorder columns for "Linkedin Worldwide" sheet
filtered_worldwide_df = filtered_worldwide_df[[
    "Date", "title", "company", "country", "link", "searched_keyword", "found_linkedin_worldwide_keywords"
]].rename(columns={"found_linkedin_worldwide_keywords": "Found Keywords"}) # Rename for clarity in sheet

print(f"Jobs for 'Linkedin Worldwide' sheet (unique and filtered): {len(filtered_worldwide_df)}")

# --- Step 5 — Process for "Count Skills" sheet ---
skill_counts = {skill: 0 for skill in count_skills_keywords}

for index, row in df_all_jobs.iterrows():
    description = row['description']
    for skill in count_skills_keywords:
        # Use regex for whole word matching
        if re.search(r'\b' + re.escape(skill) + r'\b', description, flags=re.IGNORECASE):
            skill_counts[skill] += 1

# Convert skill counts to a DataFrame
df_skill_counts_list = []
for skill, count in skill_counts.items():
    tag = skill_to_tag_map.get(skill, "Other") # Get the tag for the skill, default to "Other"
    df_skill_counts_list.append({"Date": today_date_str, "Skill": skill, "Tag": tag, "Count": count})

df_skill_counts = pd.DataFrame(df_skill_counts_list)
df_skill_counts = df_skill_counts[['Date', 'Tag', 'Skill', 'Count']] # Reorder columns to place Tag after Date

print(f"\nSkill counts for today ({today_date_str}):")
print(df_skill_counts)

# --- Step 6: Update Google Sheets ---
service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
client = gspread.authorize(credentials)

SPREADSHEET_URL = os.environ["SPREADSHEET_URL"]

# --- Update "Linkedin Worldwide" Sheet ---
WORKSHEET_NAME_WORLDWIDE = 'Linkedin Worldwide'
try:
    sheet_worldwide = client.open_by_url(SPREADSHEET_URL).worksheet(WORKSHEET_NAME_WORLDWIDE)
except gspread.WorksheetNotFound:
    sheet_worldwide = client.open_by_url(SPREADSHEET_URL).add_worksheet(title=WORKSHEET_NAME_WORLDWIDE, rows="1000", cols="20")

print(f"\nUpdating '{WORKSHEET_NAME_WORLDWIDE}' sheet...")
sheet_worldwide.clear() # Clear existing data
sheet_worldwide.update(
    [filtered_worldwide_df.columns.values.tolist()] +
    filtered_worldwide_df.values.tolist()
)
print(f"✅ Data successfully updated in '{WORKSHEET_NAME_WORLDWIDE}'!")

# --- Update "Count Skills" Sheet ---
WORKSHEET_NAME_COUNT_SKILLS = 'Count Skills'
try:
    sheet_count_skills = client.open_by_url(SPREADSHEET_URL).worksheet(WORKSHEET_NAME_COUNT_SKILLS)
except gspread.WorksheetNotFound:
    sheet_count_skills = client.open_by_url(SPREADSHEET_URL).add_worksheet(title=WORKSHEET_NAME_COUNT_SKILLS, rows="1000", cols="20")

print(f"\nUpdating '{WORKSHEET_NAME_COUNT_SKILLS}' sheet...")

# Get existing data from the sheet to append
existing_data = sheet_count_skills.get_all_values()
if existing_data:
    # Check if headers match, if not, update headers
    if existing_data[0] != df_skill_counts.columns.tolist():
        sheet_count_skills.clear()
        sheet_count_skills.update([df_skill_counts.columns.tolist()])
        start_row = 2 # Start appending from the second row
    else:
        start_row = len(existing_data) + 1 # Append after existing data
else:
    # Sheet is empty, add headers first
    sheet_count_skills.update([df_skill_counts.columns.tolist()])
    start_row = 2

# Append new data
sheet_count_skills.append_rows(df_skill_counts.values.tolist(), value_input_option='RAW', insert_data_option='INSERT_ROWS')

print(f"✅ Data successfully appended to '{WORKSHEET_NAME_COUNT_SKILLS}'!")
