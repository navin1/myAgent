import logging
from pathlib import Path

import pandas as pd
import streamlit as st
import streamlit.components.v1 as _stv1

import settings

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

st.set_page_config(
    page_title="OSR Data Intelligence",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Cached initialisers ────────────────────────────────────────────────────────

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


# ── CSS ────────────────────────────────────────────────────────────────────────

_css = (Path(__file__).parent / "static" / "app.css").read_text()

st.markdown(f"<style>{_css}</style>", unsafe_allow_html=True)


# ── Session state ──────────────────────────────────────────────────────────────

if "show_runbook" not in st.session_state:
    st.session_state.show_runbook = False


# ── Fixed header ───────────────────────────────────────────────────────────────

st.markdown("""
<div id="_app_header">
  <button id="_hbtn" title="Toggle sidebar">☰</button>
  <span class="_header-title">OSR</span>
  <span class="_header-sub">Data Intelligence Platform</span>
  <div class="_header-right">
    <button id="_runbook_btn" class="_header-action">📖 Run Book</button>
  </div>
</div>
""", unsafe_allow_html=True)


# ── JS: clicks on HTML header elements map to Streamlit actions ────────────────

if "js_registered" not in st.session_state:
    st.session_state["js_registered"] = True
    _stv1.html("""
<script>
(function attach() {
  var pd = window.parent.document;
  if (!pd.body) { setTimeout(attach, 100); return; }

  // Hide native Streamlit buttons that back the HTML header actions
  function hideNativeBtn() {
    var btns = Array.from(pd.querySelectorAll('.stButton button'));
    var btn = btns.find(function(b) { return b.textContent.trim().includes('Run Book'); });
    if (btn) {
      var container = btn.closest('[data-testid="stElementContainer"]');
      if (container) container.style.display = 'none';
    }
  }
  hideNativeBtn();
  if (!pd.body.dataset.hideObserver) {
    pd.body.dataset.hideObserver = '1';
    new MutationObserver(hideNativeBtn).observe(pd.body, { childList: true, subtree: true });
  }

  if (pd.body.dataset.appListeners) return;
  pd.body.dataset.appListeners = '1';

  // Body-level delegation: survives DOM re-renders caused by Streamlit reruns
  pd.body.addEventListener('click', function(e) {
    // Hamburger toggle
    if (e.target.closest('#_hbtn')) {
      var closeBtn = pd.querySelector('[data-testid="stSidebarCollapseButton"] button')
                  || pd.querySelector('[data-testid="stSidebarCollapseButton"]');
      var openBtn  = pd.querySelector('[data-testid="collapsedControl"] button')
                  || pd.querySelector('[data-testid="collapsedControl"]')
                  || pd.querySelector('[data-testid="stExpandSidebarButton"]');
      var target = closeBtn || openBtn;
      if (target) target.click();
      return;
    }
    
    // Run Book button
    if (e.target.closest('#_runbook_btn')) {
      var rb = Array.from(pd.querySelectorAll('.stButton button')).find(function(b) { return b.innerText.includes('Run Book'); });
      if (rb) rb.click();
      return;
    }
  }, true);
})();
</script>
""", height=0)


# ── Sidebar: logo only, no menu items ─────────────────────────────────────────

with st.sidebar:
    st.markdown("""
    <div class="sidebar-logo">
      <div class="sidebar-logo-mark">OSR</div>
      <div class="sidebar-logo-sub">Data Intelligence Platform</div>
    </div>
    """, unsafe_allow_html=True)


# ── Hidden Streamlit action buttons — triggered via JS from the HTML header ───

if st.button("📖 Run Book", key="runbook_btn"):
    st.session_state.show_runbook = not st.session_state.show_runbook



_RUNBOOK_MD = (Path(__file__).parent / "runbook.md").read_text(encoding="utf-8")

# ── Main area ──────────────────────────────────────────────────────────────────

if st.session_state.show_runbook:
    with st.container(border=True):
        close_col, _ = st.columns([1, 11])
        with close_col:
            if st.button("✕ Close", key="close_runbook"):
                st.session_state.show_runbook = False
                st.rerun()
        st.markdown(_RUNBOOK_MD)

# Agent + messages
if "messages" not in st.session_state:
    st.session_state.messages = []
if "agent" not in st.session_state:
    from agent import MyAgent
    st.session_state.agent = MyAgent()

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        for tbl in msg.get("tables", []):
            st.dataframe(
                pd.DataFrame(tbl["rows"], columns=tbl["columns"]),
                hide_index=True,
                use_container_width=True,
            )

if prompt := st.chat_input("Ask anything about your data…"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            result = st.session_state.agent.ask(prompt)
        st.markdown(result["text"])
        for tbl in result.get("tables", []):
            st.dataframe(
                pd.DataFrame(tbl["rows"], columns=tbl["columns"]),
                hide_index=True,
                use_container_width=True,
            )

    st.session_state.messages.append({
        "role": "assistant",
        "content": result["text"],
        "tables": result.get("tables", []),
    })
