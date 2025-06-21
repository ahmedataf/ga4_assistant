import streamlit as st
from openai import OpenAI
from google.cloud import bigquery
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import os
import json
import pickle
from google.oauth2 import service_account
from langchain.chat_models import ChatOpenAI
from langchain.chains import RetrievalQA

# RAG components
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import OpenAIEmbeddings
from langchain.schema import Document
from langchain.text_splitter import CharacterTextSplitter

# Streamlit app config
st.set_page_config(page_title="GA4 Analytics Assistant", layout="wide")
st.title("📊 GA4 Analytics Assistant")
st.caption("Ask natural language questions about your analytics data")

# Secrets and setup
openai_client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
service_account_info = json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS"])
credentials = service_account.Credentials.from_service_account_info(service_account_info)
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
PROJECT_ID = credentials.project_id

# Load FAISS index safely
with open("index.pkl", "rb") as f:
    obj = pickle.load(f)

if isinstance(obj, tuple):
    vectorstore, _ = obj
else:
    vectorstore = obj

retriever = vectorstore.as_retriever()
retriever.search_kwargs["k"] = 2

# Retrieval QA chain
qa_chain = RetrievalQA.from_chain_type(
    llm=ChatOpenAI(model="gpt-4", temperature=0),
    retriever=retriever,
    return_source_documents=True
)

# Example function registry

def get_sessions_by_country(start_date, end_date):
    start_date_fmt = start_date.replace("-", "")
    end_date_fmt = end_date.replace("-", "")
    return f"""
    SELECT country, COUNT(*) AS session_count
    FROM `{PROJECT_ID}.ga4_sample_ai_agent.flat_sessions`
    WHERE session_start_date BETWEEN '{start_date_fmt}' AND '{end_date_fmt}'
    GROUP BY country
    ORDER BY session_count DESC
    """

function_registry = {
    "get_sessions_by_country": get_sessions_by_country,
    # Add more function mappings as needed
}

# Function parser

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

# Streamlit UI

st.markdown("#### Ask your question:")
user_input = st.text_input("Example: Which country had the most sessions in January 2021?")

if user_input:
    with st.spinner("Thinking..."):
        result = qa_chain(user_input)
        function_call = result['result']
        st.markdown(f"**🧠 GPT Function Call:** `{function_call}`")

        func, kwargs = parse_function_call(function_call)
        if func:
            sql = func(**kwargs)
            st.markdown("**📝 SQL Query:**")
            st.code(sql, language='sql')
            try:
                df = bq_client.query(sql).result().to_dataframe()
                st.success("✅ Query executed successfully.")
                st.dataframe(df)
            except Exception as e:
                st.error(f"❌ Query failed: {e}")
        else:
            st.warning("❌ Could not match the function to registry.")
