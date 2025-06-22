
import streamlit as st
from openai import OpenAI
from google.cloud import bigquery
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import os
import json
import inspect
from google.oauth2 import service_account
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_openai import OpenAIEmbeddings
from langchain.chat_models import ChatOpenAI
from langchain_community.vectorstores import FAISS

from analytics import session_functions # or from analytics import session_functions


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

# Load FAISS index
embeddings = OpenAIEmbeddings()
vectorstore = FAISS.load_local(
    "faiss_index",
    embeddings,
    allow_dangerous_deserialization=True)
retriever = vectorstore.as_retriever(search_type="similarity", search_kwargs={"k": 2})

# Custom prompt to force function-only responses
custom_prompt = PromptTemplate.from_template("""
You are an analytics assistant. You have access to predefined functions.
Each function has a name and parameters.

Respond to user questions by returning ONLY the exact Python function call.
Do NOT explain, do NOT add commentary, and DO NOT return any natural language.

Format:
function_name(param1='value1', param2='value2')

Examples:
Q: What is the bounce rate by traffic source for May 2025?
A: get_bounce_rate_traffic_source_flat_sessions(start_date='2025-05-01', end_date='2025-05-31')

Q: How many sessions came from organic traffic last month?
A: get_sessions_organic_traffic_flat_sessions(start_date='2025-05-01', end_date='2025-05-31')

Now answer:
Q: {question}
""")

# Retrieval QA chain with custom prompt
qa_chain = RetrievalQA.from_chain_type(
    llm=ChatOpenAI(model="gpt-4", temperature=0),
    retriever=retriever,
    chain_type_kwargs={"prompt": custom_prompt},
    return_source_documents=False
)

# Dynamically register all functions from session_functions
function_registry = {
    name: fn
    for name, fn in inspect.getmembers(session_functions, inspect.isfunction)
    if name.startswith("get_")
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
        # Directly get the function call string
        function_call = qa_chain.run(user_input)
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
