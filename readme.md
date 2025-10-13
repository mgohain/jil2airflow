## Autosys JIL to Airflow DAG Converter
### Creating and Using a Virtual Environment

It is recommended to use a Python virtual environment to manage dependencies:

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

Once activated, proceed with the installation steps below.
### Installation

```bash
pip install -r requirements.txt
```

### Usage

Start the Streamlit app:

```bash
streamlit run streamlit_app.py --browser.gatherUsageStats false --server.address 0.0.0.0 --server.port 8080
```

### Features
- Upload Autosys JIL files and convert them to Airflow DAGs
- Wizard-driven UI for configuration and preview
- DAG validation using Airflow's DagBag
- Conversion report and error handling

---
For more details, see the documentation in the code and Streamlit UI.