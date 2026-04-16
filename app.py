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

keywords_for_scraping = ["Morocco","South Africa","Mauritius","Seychelles",
             "Qatar","Oman","Kuwait","Bahrain","Saudi Arabia","United Arab Emirates","Jordan",
             "Japan", "South Korea","Hong Kong SAR", "Singapore","Australia","New Zealand",
             "Turkey","Bosnia and Herzegovina","Georgia","Albania","Ukraine","Russia","Canada",
            "Portugal", "Malta", "Romania", "Czech Republic", "Latvia", "Lithuania", "Hungary", "Slovakia", "Cyprus","Luxembourg",
             "Iceland","Greenland","Switzerland","Estonia","Denmark","Finland","Sweden","Norway", "European Economic Area"
            ]
excluded_countries = ["United States", "USA", "États-Unis", "India", "Pakistan","Philippines","Israel","Vietnam"]

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
        "looker", "qlik",
        # New trending skills
        "Power Automate Desktop","Google Looker Studio","Apache Superset",
        "Data storytelling","ObservableHQ","ThoughtSpot","Microsoft Fabric",
        "Python Dash","Streamlit Components","BI embedded analytics",
        "AI-powered dashboards","Real-time analytics","Causal inference",
        "Bayesian modeling","Advanced Excel functions","Snowflake Data Sharing",
        "DBT for analytics","Data quality monitoring","Dataprep/ETL automation",
        "Analytics for generative AI insights"
    ],
    "Data Engineer": [
        "etl", "airflow", "dbt", "spark", "kafka", "hadoop", "snowflake", "databricks", "redshift", "bigquery",
        "batch processing", "stream processing", "data modeling", "data pipelines",
        # New trending skills
        "Delta Lake","Apache Iceberg","Feast Feature Store","Prefect","Dagster",
        "Flink SQL","Event-driven pipelines","Data mesh patterns","Lakehouse architecture",
        "StreamSets","MLflow","Cloud-native ETL","PolyBase",
        "Data observability","Airbyte","OpenLineage","Temporal workflows","Graph pipelines",
      
    ],
    "Data Scientist": [
        "data science", "machine learning", "predictive modeling", "model evaluation",
        "statistics", "nlp", "computer vision","ML", "deep learning", "dl",
        "scikit-learn", "tensorflow", "pytorch", "keras", "xgboost", "lightgbm",
        "time series", "a/b testing",
        # New trending skills
        "Hugging Face ","AutoML ","MLOps","Few-shot learning",
        "Reinforcement learning","Diffusion models","Graph neural networks",
        "Deep reinforcement learning","Model interpretability","Synthetic data generation",
        "TinyML","Generative design","Edge AI","Transfer learning pipelines",
        "Prompt engineering","Active learning loops","Multimodal AI",
        "Foundation model fine-tuning","AI-assisted feature engineering"
    ],
    "AI/ML Engineer": [
        "artificial intelligence", "ai", "llm", "large language models", "prompt engineering",'context engineering',
        "fine-tuning","fine tuning", "rag","generative ai", "genai", "gen ai",
        "retrieval augmented generation", "hugging face", "openai", "gemini",
        "deepseek", "claude", "transformers", "bert", "llama","ollama",
       
        # New trending skills
       "LangChain","Langraph", "crewai","tenseflow","keras","sckit-learn","sckit learn",
   
    ],
    "AI Automation/RPA/No-Code": [
        "n8n", "zapier", "make.com", "integromat", "uipath", "power automate",
        "power apps", "mendix", "automation anywhere", "rpa", "appian",
        # New trending skills
      "Bubble", "Webflow", "Framer", "Glide", "Retool",
        "Lovable", "Bolt", "Replit",
        "Claude Code", "Claude Cowork", "AI studio","workato"
    ],
  
    "Programming Languages": [
        "python", "javascript", "go", "java", "c#", "scala",
        "kotlin", "swift", "php", "ruby", "rust", "HTML", "CSS", "sql","nosql",
        "typescript", "bash", "shell scripting", "R",
    ],
    "DevOps": [
        "docker", "kubernetes", "ci/cd", "github actions", "jenkins",
        "terraform", "ansible", "helm", "prometheus", "grafana", "git"
    ],

  
    "Cloud Platforms": [
        "aws", "azure", "gcp",
        # New trending skills
        "AWS Bedrock","Azure OpenAI Service","GCP Vertex AI"

    ],
    "Databases": [
        "postgresql", "mysql", "mongodb", "redis", "cassandra","sqlite",
        # New trending skills
        "Neo4j","Amazon QLDB","CockroachDB","YugabyteDB","TimescaleDB","DynamoDB","cloud-native DB",
            # Vector (for AI)
        "Pinecone", "Weaviate", "Qdrant", "pgvector",
    ],
  
    "Frontend/UI/UX": [
        "React", "Next.js", "Vue.js", "Nuxt.js", "Angular","SvelteKit", "SolidJS", "Astro",
        # New trending skills
         "TailwindCSS", "CSS animations", "Framer Motion", "GSAP",
        # Design tools
        "Figma", "Adobe XD", "Canva", "Framer",
        # 3D & advanced UI
        "Three.js", "WebGL", "React Three Fiber",
    ],
  
    "Backend/API Development": [
        "node.js", "express.js","nestjs","ASP.net"
        "spring boot", "django", "flask", "fastapi", "asp.net","Deno",
        "REST API", "GraphQL", "GraphQL federation", "gRPC", "tRPC", "Async",
        "WebSockets", "Kafka Streams API", "OAuth 2.0", "JWT", "API versioning"
    ],
  
    "Project / Product Management": [
        # Methodologies
        "Agile", "Scrum", "Kanban", "Scrum@Scale", "SAFe",
        "hybrid project frameworks", "OKRs",
        # Tools
        "Jira", "Linear", "ClickUp", "Notion", "Asana",
        "Jira Advanced Automations", "ClickUp AI automations",
        # Skills
        "product roadmapping", "backlog grooming", "stakeholder management",
        "data-driven KPIs", "risk management",
        "product analytics", "Figma-to-Jira design handoff",
        "DevOps project tracking", "process mining",
        "AI product experimentation",
    ],
 
    # ─────────────────────────────────────────────
    # CYBERSECURITY  (new — high demand)
    # ─────────────────────────────────────────────
    "Cybersecurity": [
        "penetration testing", "ethical hacking", "OWASP Top 10",
        "SIEM (Splunk, Microsoft Sentinel)", "SOC operations",
        "vulnerability assessment", "zero-trust architecture",
        "IAM (Identity & Access Management)", "OAuth 2.0 / OIDC",
        "API security", "cloud security (CSPM)", "container security",
        "DevSecOps", "secrets management",
        "threat modeling", "incident response",
        "AI-powered threat detection",
    ],
 
    # ─────────────────────────────────────────────
    # BUSINESS INTELLIGENCE & STRATEGY (new)
    # ─────────────────────────────────────────────
    "Business Intelligence & Strategy": [
        "market research", "competitive analysis", "strategic planning",
        "business case writing", "ROI",
        "decision making", "reporting","KPI",
        "veille stratégique",
        "Lean Six Sigma", "HSE",
        "ISO", "QHSE", "ESG",
    ],

  "Ads / Growth Marketing": [
    # Platforms
    "Google Ads", "Meta Ads", "Facebook Ads", "Instagram Ads", "TikTok Ads", "LinkedIn Ads",
    "Chatgpt ads",

    # Tracking & Analytics
    "Google Tag Manager", "GTM", "Conversion Tracking", "Pixel tracking",
    "Google Analytics 4", "GA4"
  ],

  "ERP Systems": [
    # Major ERP Platforms
    "SAP", "Salesforce", "Oracle ERP",
    "Microsoft Dynamics", "Dynamics 365", "Odoo", "NetSuite",
    "Salesforce", "HubSpot","Zoho", "monday CRM",
    "Pipedrive", "Freshsales",
  ],

  
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

for keyword in keywords_for_scraping:
    for i in range(0, 30):  # Increase range for more pages
        url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keyword}&location={keyword}&f_TPR=r86400&start={i*25}"
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
