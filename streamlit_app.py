import streamlit as st
from openai import OpenAI
from google.cloud import bigquery
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import os
import json
from google.oauth2 import service_account
import pickle

# LangChain & RAG imports
from langchain.chat_models import ChatOpenAI
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import OpenAIEmbeddings

# Streamlit config
st.set_page_config(page_title="GA4 Analytics Assistant", layout="wide")
st.title("📊 GA4 Analytics Assistant")
st.caption("Ask natural language questions about your analytics data")

# Load credentials
openai_client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
service_account_info = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS"])
credentials = service_account.Credentials.from_service_account_info(service_account_info)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
PROJECT_ID = credentials.project_id

# Load FAISS index
with open("index.pkl", "rb") as f:
    vectorstore = pickle.load(f)
retriever = vectorstore.as_retriever()
retriever.search_kwargs["k"] = 2


# Define RAG RetrievalQA chain
llm = ChatOpenAI(model="gpt-4", temperature=0)
custom_prompt = PromptTemplate.from_template("""
You are a SQL assistant. Based on the user's question, return the correct Python function name and arguments from available documentation. 
Do not explain. Return only one line in the following format:

Function: <function_name>(arg1=value1, arg2=value2)

User question: {query}
""")

qa_chain = RetrievalQA.from_chain_type(
    llm=llm,
    retriever=retriever,
    chain_type_kwargs={"prompt": custom_prompt},
    return_source_documents=True
)

def get_function_call_from_rag(user_input):
    response = qa_chain(user_input)
    return response["result"]

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

def resolve_dates_in_kwargs(kwargs):
    resolved = kwargs.copy()
    for key, value in kwargs.items():
        if isinstance(value, str):
            try:
                start, end = resolve_date_range(value)
                if "start" in key.lower():
                    resolved["start_date"] = start
                elif "end" in key.lower():
                    resolved["end_date"] = end
            except Exception:
                pass
    return resolved

def run_query(sql):
    job = bq_client.query(sql)
    return job.result().to_dataframe()

# Placeholder function registry (must be updated with full SQL functions)
def get_sessions_by_country(country, start_date, end_date):
    start_date_fmt = start_date.replace("-", "")
    end_date_fmt = end_date.replace("-", "")
    return f"""
    SELECT country, COUNT(*) AS session_count
    FROM `{PROJECT_ID}.ga4_sample_ai_agent.flat_sessions`
    WHERE session_start_date BETWEEN '{start_date_fmt}' AND '{end_date_fmt}'
      AND country = '{country}'
    GROUP BY country
    ORDER BY session_count DESC
    """

function_registry = {
    "get_sessions_by_country": get_sessions_by_country,
    # Add all additional functions here
}

# Streamlit UI
st.markdown("#### Ask your question:")
user_input = st.text_input("Example: What was the bounce rate last month?")

if user_input:
    with st.spinner("Thinking..."):
        gpt_output = get_function_call_from_rag(user_input)
        st.markdown(f"**🧠 RAG Function Call:** `{gpt_output}`")

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
