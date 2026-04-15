import logging

import pandas as pd
import streamlit as st

import settings

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

st.set_page_config(page_title="MyAgent", page_icon="🔗", layout="centered")


@st.cache_resource(show_spinner="Connecting to BigQuery…")
def init_bq() -> int:
    import bq_service
    return sum(len(bq_service.list_datasets(p).get("datasets", [])) for p in settings.BQ_PROJECTS)


@st.cache_resource(show_spinner="Loading Excel files…")
def init_excel() -> int:
    if not settings.EXCEL_DATA_PATH:
        return 0
    import excel_service
    result = excel_service.load_excel_files()
    if "error" in result:
        st.warning(f"Excel: {result['error']}")
    return result.get("loaded", 0)


try:
    total_datasets = init_bq()
except Exception as exc:
    st.error(f"Failed to connect to BigQuery: {exc}")
    st.stop()

total_excel_tables = init_excel()

st.markdown("<h1 style='color: orange;'>MyAgent</h1>", unsafe_allow_html=True)
st.markdown("<h3 style='color: grey;'>My personal assistant..</h3>", unsafe_allow_html=True)
st.divider()
caption_parts = [f"Projects: {', '.join(f'`{p}`' for p in settings.BQ_PROJECTS)} — {total_datasets} dataset(s)"]
if total_excel_tables:
    caption_parts.append(f"Excel: {total_excel_tables} table(s) from `{settings.EXCEL_DATA_PATH}`")
st.caption(" · ".join(caption_parts))
st.divider()

if "messages" not in st.session_state:
    st.session_state.messages = []
if "agent" not in st.session_state:
    from agent import MyAgent
    st.session_state.agent = MyAgent()

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        for tbl in msg.get("tables", []):
            st.dataframe(pd.DataFrame(tbl["rows"], columns=tbl["columns"]), hide_index=True, use_container_width=True)

if prompt := st.chat_input("Ask anything about your BigQuery data…"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            result = st.session_state.agent.ask(prompt)
        st.markdown(result["text"])
        for tbl in result.get("tables", []):
            st.dataframe(pd.DataFrame(tbl["rows"], columns=tbl["columns"]), hide_index=True, use_container_width=True)

    st.session_state.messages.append({"role": "assistant", "content": result["text"], "tables": result.get("tables", [])})
