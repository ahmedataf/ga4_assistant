
import streamlit as st
from openai import OpenAI
from google.cloud import bigquery
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import os
import json
from google.oauth2 import service_account

st.set_page_config(page_title="GA4 Analytics Assistant", layout="wide")
st.title("📊 GA4 Analytics Assistant")
st.caption("Ask natural language questions about your analytics data")

openai_client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
service_account_info = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS"])
credentials = service_account.Credentials.from_service_account_info(service_account_info)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
PROJECT_ID = credentials.project_id

GPT_PROMPT_TEMPLATE = """You are a data assistant that maps user questions to function calls.

Here are examples:

User: What was the bounce rate in January?
Function: get_bounce_rate(start_date='2021-01-01', end_date='2021-01-31')

User: How many sessions came from the United Arab Emirates in March?
Function: get_sessions_by_country(country='United Arab Emirates', start_date='2021-03-01', end_date='2021-03-31')

User: Which pages had the most views in January?
Function: get_top_pages(start_date='2021-01-01', end_date='2021-01-31', limit=10)

User: How many sessions came from different devices in March?
Function: get_sessions_by_device(start_date='2021-03-01', end_date='2021-03-31')

User: What was the revenue by country in January?
Function: get_revenue_by_country(start_date='2021-01-01', end_date='2021-01-31')

User: {user_input}
Function:
"""

def get_function_call_from_gpt(user_input):
    prompt = GPT_PROMPT_TEMPLATE.format(user_input=user_input)
    response = openai_client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a helpful assistant for analytics."},
            {"role": "user", "content": prompt}
        ],
        temperature=0
    )
    return response.choices[0].message.content.strip()

def parse_function_call(call_str):
    match = re.match(r"(\w+)\((.*)\)", call_str)
    if not match:
        return None, None
    func_name = match.group(1)
    args_str = match.group(2)
    kwargs = {}
    for part in args_str.split(","):
        if "=" in part:
            key, value = part.split("=", 1)
            kwargs[key.strip()] = value.strip().strip("'").strip('"')
    return function_registry.get(func_name), kwargs

def resolve_date_range(phrase):
    today = datetime.today()
    phrase = phrase.lower()
    if "last month" in phrase:
        start = (today.replace(day=1) - relativedelta(months=1)).replace(day=1)
        end = start.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
    elif "this month" in phrase:
        start = today.replace(day=1)
        end = (start + relativedelta(months=1)) - relativedelta(days=1)
    elif "last week" in phrase:
        start = today - relativedelta(days=today.weekday() + 7)
        end = start + relativedelta(days=6)
    elif "this week" in phrase:
        start = today - relativedelta(days=today.weekday())
        end = start + relativedelta(days=6)
    elif "past 7 days" in phrase:
        end = today
        start = end - relativedelta(days=6)
    elif match := re.match(r"q(\d)\s+(\d{4})", phrase):
        q, year = int(match[1]), int(match[2])
        start_month = (q - 1) * 3 + 1
        start = datetime(year, start_month, 1)
        end = (start + relativedelta(months=3)) - relativedelta(days=1)
    else:
        start = today.replace(day=1)
        end = (start + relativedelta(months=1)) - relativedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def is_date_expression(value):
    """Returns True if value is a natural date phrase (not a YYYY-MM-DD format)."""
    if re.match(r"\d{4}-\d{2}-\d{2}", value):
        return False
    return any(kw in value.lower() for kw in [
        "last month", "this month", "last week", "this week",
        "past 7 days", "q1", "q2", "q3", "q4"
    ])

def resolve_dates_in_kwargs(kwargs):
    resolved = kwargs.copy()
    for key, value in kwargs.items():
        if isinstance(value, str) and is_date_expression(value):
            try:
                start, end = resolve_date_range(value)
                if "start" in key.lower():
                    resolved["start_date"] = start
                elif "end" in key.lower():
                    resolved["end_date"] = end
                else:
                    # If neither key says start/end, we infer both
                    resolved["start_date"] = start
                    resolved["end_date"] = end
            except Exception:
                pass
    return resolved


def run_query(sql):
    job = bq_client.query(sql)
    return job.result().to_dataframe()

# Query functions
def get_bounce_rate(start_date, end_date):
    return f"""
    SELECT SAFE_DIVIDE(
      SUM(CASE WHEN pageviews = 1 THEN 1 ELSE 0 END),
      COUNT(*)
    ) AS bounce_rate
    FROM `{PROJECT_ID}.ga4_sample_ai_agent.flat_sessions`
    WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
    """

def get_sessions_by_country(country, start_date, end_date):
    return f"""
    SELECT country, COUNT(*) AS session_count
    FROM `{PROJECT_ID}.ga4_sample_ai_agent.flat_sessions`
    WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
      AND country = '{country}'
    GROUP BY country
    ORDER BY session_count DESC
    """

def get_top_pages(start_date, end_date, limit=10):
    return f"""
    SELECT page_title, COUNT(*) AS views
    FROM `{PROJECT_ID}.ga4_sample_ai_agent.flat_pages`
    WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY page_title
    ORDER BY views DESC
    LIMIT {limit}
    """

def get_sessions_by_device(start_date, end_date):
    return f"""
    SELECT device_category, COUNT(*) AS sessions
    FROM `{PROJECT_ID}.ga4_sample_ai_agent.flat_sessions`
    WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY device_category
    ORDER BY sessions DESC
    """

def get_revenue_by_country(start_date, end_date):
    return f"""
    SELECT country, SUM(purchase_value) AS total_revenue
    FROM `{PROJECT_ID}.ga4_sample_ai_agent.flat_conversions`
    WHERE event_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY country
    ORDER BY total_revenue DESC
    """

def get_top_countries_by_sessions(start_date, end_date, limit=5):
    return f"""
    SELECT country, COUNT(*) AS session_count
    FROM `{PROJECT_ID}.ga4_sample_ai_agent.flat_sessions`
    WHERE session_start_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY country
    ORDER BY session_count DESC
    LIMIT {limit}
    """

function_registry = {
    "get_bounce_rate": get_bounce_rate,
    "get_sessions_by_country": get_sessions_by_country,
    "get_top_pages": get_top_pages,
    "get_sessions_by_device": get_sessions_by_device,
    "get_revenue_by_country": get_revenue_by_country,
    "get_top_countries_by_sessions": get_top_countries_by_sessions,
}

st.markdown("#### Ask your question:")
user_input = st.text_input("Example: What was the bounce rate last month?")

if user_input:
    with st.spinner("Thinking..."):
        gpt_output = get_function_call_from_gpt(user_input)
        st.markdown(f"**🧠 GPT Function Call:** `{gpt_output}`")

        func, kwargs = parse_function_call(gpt_output)

        if func:
            resolved_kwargs = resolve_dates_in_kwargs(kwargs)
            sql = func(**resolved_kwargs)
            st.markdown("**📝 SQL Query:**")
            st.code(sql, language='sql')

            try:
                df = run_query(sql)
                st.success("✅ Query executed successfully.")
                st.dataframe(df)
            except Exception as e:
                st.error(f"❌ Query failed: {e}")
        else:
            st.warning("❌ I couldn't match that to a known function.")
