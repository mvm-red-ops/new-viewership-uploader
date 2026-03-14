"""
Viewership Data Tools Launcher
Multi-utility application for viewership data management
"""

import streamlit as st
import sys
import os

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import get_environment_name

# Page configuration
st.set_page_config(
    page_title="Viewership Data Tools",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Environment indicator in sidebar
env_name = get_environment_name()
env_color = "green" if env_name == "PRODUCTION" else "blue"
st.sidebar.markdown(f"### Environment: :{env_color}[{env_name}]")
st.sidebar.divider()

# Create navigation
st.sidebar.title("📊 Viewership Tools")

# Define pages
discrepancy_checker_page = st.Page(
    "pages/discrepancy_checker.py",
    title="Discrepancy Checker",
    icon="🔍",
    default=False
)

# Navigation structure
pg = st.navigation({
    "Tools": [
        discrepancy_checker_page,
    ]
})

# Add information in sidebar
st.sidebar.markdown("""
### About
This application provides utilities for managing and validating viewership data.

**Available Tools:**
- **Discrepancy Checker**: Compare staging vs final table data

**Note:** For data upload, use `streamlit run app.py`
""")

# Run the selected page
pg.run()
