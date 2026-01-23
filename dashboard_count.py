import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import os
import json
import plotly.express as px

# --- PAGE CONFIGURATION ---
st.set_page_config(page_title="Skills Dashboard", layout="wide")

# --- AUTHENTICATION & DATA LOADING ---
@st.cache_data(ttl=600)
def load_data():
    try:
        # 1. Define Scopes
        SCOPES = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        # 2. Load Credentials from Environment Variable
        # We check st.secrets first (Streamlit Cloud specific), then fall back to os.environ
        if "GOOGLE_SERVICE_ACCOUNT" in st.secrets:
             service_account_info = st.secrets["GOOGLE_SERVICE_ACCOUNT"]
             # If st.secrets parses it as a dict automatically, use it directly
             # If it's a string, load it as json
             if isinstance(service_account_info, str):
                 service_account_info = json.loads(service_account_info)
        else:
            # Your requested method:
            service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])

        credentials = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
        client = gspread.authorize(credentials)

        # 3. Get Spreadsheet URL
        if "SPREADSHEET_URL" in st.secrets:
            spreadsheet_url = st.secrets["SPREADSHEET_URL"]
        else:
            spreadsheet_url = os.environ["SPREADSHEET_URL"]

        # 4. Open Sheet and Worksheet
        # Using 'test1' as requested
        sheet = client.open_by_url(spreadsheet_url).worksheet("Count Skills")
        
        # 5. Get data
        data = sheet.get_all_records()
        df = pd.DataFrame(data)

        # 6. Data Cleaning
        # Convert Count to numeric, handle errors
        if 'Count' in df.columns:
            df['Count'] = pd.to_numeric(df['Count'], errors='coerce').fillna(0)
        
        # Convert Date if exists
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])

        return df

    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

# --- MAIN APP LOGIC ---
st.title("📊 Skills & Categories Visualization")

df = load_data()

if not df.empty:
    # --- SIDEBAR FILTERS ---
    st.sidebar.header("Filters")
    
    # Date Filter
    if 'Date' in df.columns:
        # Convert dates to string for the dropdown to look nice
        dates = df['Date'].dt.date.unique()
        # Sort dates descending (newest first)
        dates = sorted(dates, reverse=True)
        selected_date = st.sidebar.selectbox("Select Date", dates, index=0)
        
        # Filter dataframe
        df_filtered = df[df['Date'].dt.date == selected_date]
    else:
        df_filtered = df

    # --- TABS FOR DIFFERENT VIEWS ---
    tab1, tab2 = st.tabs(["🌎 All Categories", "🔍 Category Deep Dive"])

    # === TAB 1: ALL CATEGORIES ===
    with tab1:
        st.subheader("Overview by Category")
        
        # Aggregate data: Sum counts per Tag
        cat_group = df_filtered.groupby("Tag")["Count"].sum().reset_index().sort_values(by="Count", ascending=True)

        # Chart: Total Counts per Category
        fig_cat = px.bar(
            cat_group,
            x="Count",
            y="Tag",
            orientation='h',
            text="Count",
            title="Total Skills Count per Category",
            color="Count",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig_cat, use_container_width=True)

    # === TAB 2: DEEP DIVE ===
    with tab2:
        st.subheader("Skills inside a Category")
        
        # Dropdown to pick a category
        unique_tags = df_filtered['Tag'].unique()
        selected_tag = st.selectbox("Select Category:", unique_tags)
        
        # Filter for that tag
        skill_df = df_filtered[df_filtered['Tag'] == selected_tag].sort_values(by="Count", ascending=True)
        
        # Chart: Skills in that category
        if not skill_df.empty:
            fig_skills = px.bar(
                skill_df,
                x="Count",
                y="Skill",
                orientation='h',
                text="Count",
                title=f"Skill Breakdown for: {selected_tag}",
                height=600, # Taller chart to accommodate many skills
                color="Count",
                color_continuous_scale="Viridis"
            )
            st.plotly_chart(fig_skills, use_container_width=True)
            
            # Show raw data table below chart
            with st.expander("View Data Table"):
                st.dataframe(skill_df[['Skill', 'Count']].sort_values(by="Count", ascending=False))
        else:
            st.info("No skills found for this category.")

else:
    st.warning("No data loaded. Please check your environment variables and spreadsheet permissions.")
