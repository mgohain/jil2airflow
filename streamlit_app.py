# ---------------- refactored_streamlit_wizard_app.py ----------------
import streamlit as st
import json
import zipfile
import io
from streamlit_ace import st_ace
from jil_parser import JILParser
from airflow_dag_generator import AirflowDAGGenerator
import re
from summary_and_visualization import show_summary_and_dag
from dag_configurator import show_single_dag_configurator
import os
from  utils.external_dep_utils import ExternalDepUtils

# ----------------------------------------
# Session Init
# ----------------------------------------
def init_session():
    defaults = {
        "step": 0,
        "mode": None,
        "jil_files": [],
        "jil_content": "",
        "jobs_dict": None,
        "batch_jobs_dicts": {},
        "batch_dags": {},
        "dag_id": "autosys_converted_dag",
        "schedule": "DEFAULT",
        "dag_generated": False,
        "dag_code": "",
        "dag_code_version": 0,
        "selected_dag_to_view": None,
        "validation_failed": False,
        "ext_dep_dict": {},
        "ext_option": None,
        "jil_files_full_name": []
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session()
st.set_page_config(page_title="JIL to Airflow Wizard", layout="wide")
st.title("🧙‍♂️ Autosys JIL → Airflow DAG Wizard")

# ----------------------------------------
# Function to Substitute Environment Variables
# ----------------------------------------
def substitute_env_vars(command: str, env_vars: dict) -> str:
    # Substitute $var and ${var} with their values from env_vars
    def replacer(match):
        var_name = match.group(1) or match.group(2)
        return env_vars.get(var_name, match.group(0))
    return re.sub(r'\$(\w+)|\$\{(\w+)\}', replacer, command)

# ----------------------------------------
# Sidebar for Environment Variables
# ----------------------------------------
with st.sidebar:
    st.header("Profile / Environment Variables")
    # Checkbox for substitution
    substitute_in_command = st.checkbox("Substitute in command", value=st.session_state.get("substitute_in_command", False), key="substitute_in_command")
    # Do NOT assign to st.session_state.substitute_in_command here
    if "env_vars" not in st.session_state:
        st.session_state.env_vars = {}
    if "env_vars_list" not in st.session_state:
        st.session_state.env_vars_list = list(st.session_state.env_vars.items())

    if st.button("➕ Add Variable"):
        # Only append if not both key and value are blank
        if not (len(st.session_state.env_vars_list) > 0 and st.session_state.env_vars_list[-1][0] == "" and st.session_state.env_vars_list[-1][1] == ""):
            st.session_state.env_vars_list.append(("", ""))

    for i, (key, val) in enumerate(st.session_state.env_vars_list):
        cols = st.columns([5, 8, 2])  # Adjusted column widths
        new_key = cols[0].text_input("Key", value=key, key=f"env_key_{i}")
        new_val = cols[1].text_input("Value", value=val, key=f"env_val_{i}")
        st.session_state.env_vars_list[i] = (new_key, new_val)  # Moved this line before delete button
        
        # Center the delete button using markdown with adjusted padding
        cols[2].markdown("<div style='text-align: center; padding-top: 28px'>", unsafe_allow_html=True)
        if cols[2].button("🗑️", key=f"del_env_{i}"):
            st.session_state.env_vars_list.pop(i)
            st.rerun()
        cols[2].markdown("</div>", unsafe_allow_html=True)

    # Only keep pairs where at least key or value is non-blank
    st.session_state.env_vars = {k: v for k, v in st.session_state.env_vars_list if k or v}
    #print(f"{st.session_state.env_vars_list} {st.session_state.env_vars}")

# ----------------------------------------
# Step 0: Mode Selection + Upload
# ----------------------------------------
if st.session_state.step == 0:
    st.header("Step 0: Select Mode and Upload File(s)")
    mode = st.radio("Choose Mode", ["Single File Mode", "Batch File Mode"])
    st.session_state.mode = "single" if "Single" in mode else "batch"
    multiple = st.session_state.mode == "batch"
    # files = st.file_uploader("Upload JIL files", type=["jil", "txt"], accept_multiple_files=multiple)
    files = st.file_uploader("Upload JIL files", type=["jil", "txt"], accept_multiple_files=True, key="step0_file_uploader")

    if files:
        st.session_state.jil_files = [os.path.splitext(file.name)[0] for file in files]
        st.session_state.jil_files_full_name = [file.name for file in files]
        if not isinstance(files, list):
            files = [files]
        parsed_files = {}
        ext_dep_dict = {}
        parser = None
        if st.session_state.mode == "single":
            parser = JILParser()
        for f in files:
            try:
                if st.session_state.mode == "batch":
                    parser = JILParser()
                content = f.read().decode("utf-8")
                jobs = parser.parse_content(content)
                # Always pass env vars as job.envvars
                for job in jobs.values():
                    if hasattr(job, "command") and job.command:
                        if hasattr(job, "envvars") and isinstance(job.envvars, dict):
                            #print(f"current job.envvars={job.envvars}")
                            job.envvars.update(st.session_state.env_vars)
                            #print(f"update job.envvars={st.session_state.env_vars}")
                        else:
                            #print(f"current job.envvars={job.envvars}")
                            job.envvars = dict(st.session_state.env_vars)
                            #print(f"add job.envvars={st.session_state.env_vars}")
                # Substitute in command if checked
                if st.session_state.get("substitute_in_command", False):
                    for job in jobs.values():
                        if hasattr(job, "command") and job.command:
                            job.command = substitute_env_vars(job.command, st.session_state.env_vars)
                        if hasattr(job, "watch_file") and job.watch_file:
                            job.watch_file = substitute_env_vars(job.watch_file, st.session_state.env_vars)
                if not jobs:
                    st.warning(f"⚠️ No jobs found in: {f.name}")
                    continue
                    
                parsed_files[f.name] = jobs
                #------------------------------------------
                # CHecking for external dependency
                #------------------------------------------
                ext_dep_to_job_map = ExternalDepUtils.extract_external_dependency_to_job_mapping(jobs)  
                if ext_dep_to_job_map:
                    ext_dep_dict[f.name] = list(ext_dep_to_job_map.keys())
                #-----------External dependency check ends
            except Exception as e:
                st.error(f"❌ Error parsing {f.name}: {e}")	
        if parsed_files:
            if st.session_state.mode == "single":
                st.session_state.jil_content = content
                st.session_state.batch_jobs_dicts = parsed_files
                st.session_state.jobs_dict = next(iter(parsed_files.values()))                          
            else:
                st.session_state.batch_jobs_dicts = parsed_files
            #------------------- Redirecting if external dependency found-------   
            if ext_dep_dict:
                st.session_state.validation_failed = True
                st.session_state.ext_dep_dict = ext_dep_dict
                if st.session_state.mode == "batch":
                    st.session_state.step = -1
                    st.rerun()
                else:
                    st.session_state.step = 9
                    st.rerun()
            #-------------------Redirection ends-------------
            cols = st.columns([3, 1, 3])
            with cols[0]:
                if st.button("Next ➡️", key="step0_next"):
                    st.session_state.step = 1
                    st.rerun()
    else:
        cols = st.columns([3, 1, 3])
        with cols[0]:
            st.button("Next ➡️", key="step0_next_nofile", disabled=True)

# ----------------------------------------
# Step 1 (Single): Summary + Visualization
# ----------------------------------------
elif st.session_state.step == 1 and st.session_state.mode == "single":
    show_summary_and_dag(st.session_state.jobs_dict)
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step1s_back"):
            st.session_state.step = 0
            st.rerun()
    with cols[1]:
        if st.button("Next ➡️", key="step1s_next"):
            st.session_state.step = 2
            st.rerun()

# ----------------------------------------
# Step 1 (Batch): Common Configuration
# ----------------------------------------
elif st.session_state.step == 1 and st.session_state.mode == "batch":
    st.header("Step 1: Common Configuration for All DAGs")
    st.session_state.schedule = st.text_input("Schedule Interval (cron/@daily etc.)", value=st.session_state.schedule)

    # Operator configuration
    operator_type = st.selectbox("Operator Type", ["SSHOperator", "KubernetesPodOperator", "SQLExecuteQueryOperator"])
    st.session_state.operator_type = operator_type
    if operator_type == "KubernetesPodOperator":
        image = st.text_input("Container Image", value="python:3.9")
        namespace = st.text_input("Namespace", value="default")
        envvars = st.text_input("Env Vars (JSON)", value=json.dumps(st.session_state.env_vars))
        resource_requests = st.text_input("Resource Requests", value="")
        resource_limits = st.text_input("Resource Limits", value="")
    elif operator_type == "SQLExecuteQueryOperator":
        db_conn_id = st.text_input("DB Connection ID (defaults to job's machine attribute if present)", value="DEFAULT")
        envvars = st.text_input("Environment (JSON)", value=json.dumps(st.session_state.env_vars))
    else:
        # For SSHOperator, show SSH Connection ID but default to machine attribute if present
        ssh_conn_id = st.text_input("SSH Connection ID (defaults to job's machine attribute if present)", value="DEFAULT")
        envvars = st.text_input("Environment (JSON)", value=json.dumps(st.session_state.env_vars))
    #st.session_state.dag_id = st.text_input("Base DAG ID", value=st.session_state.dag_id)
    if st.button("Generate All DAGs 🚀"):
        # Inject operator config into each job
        for fname, jobs_dict in st.session_state.batch_jobs_dicts.items():
            for job in jobs_dict.values():
                if job.has_command():
                    job.operator_type = operator_type
                    if operator_type == "KubernetesPodOperator":
                        job.command_image = image
                        job.namespace = namespace
                        job.envvars = json.loads(envvars)
                        job.resource_requests = resource_requests
                        job.resource_limits = resource_limits
                    elif operator_type == "SQLExecuteQueryOperator":
                        job.db_conn_id = db_conn_id
                        job.envvars = json.loads(envvars)
                    else:
                        job.ssh_conn_id = ssh_conn_id
                        job.envvars = json.loads(envvars)
        for fname, jobs_dict in st.session_state.batch_jobs_dicts.items():
            dag_id = f"{fname.split('.')[0]}"
            dag_code = AirflowDAGGenerator(jobs_dict).generate_dag(dag_id, st.session_state.schedule)
            st.session_state.batch_dags[fname] = {
                "dag_id": dag_id,
                "code": dag_code,
                "jobs_dict": jobs_dict
            }
        st.session_state.step = 2
        st.rerun()
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step1b_back"):
            st.session_state.step = 0
            st.rerun()
    with cols[1]:
        if st.button("🔄 Restart Wizard", key="step2_restart"):
            env_vars = st.session_state.get("env_vars", {})
            env_vars_list = st.session_state.get("env_vars_list", [])
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.env_vars = env_vars
            st.session_state.env_vars_list = env_vars_list
            st.rerun()

# ----------------------------------------
# Step 2 (Single): Configure & Generate DAG
# ----------------------------------------
elif st.session_state.step == 2 and st.session_state.mode == "single":
    show_single_dag_configurator()
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step2_back"):
            st.session_state.step = 1
            st.rerun()
    with cols[1]:
        if st.button("🔄 Restart Wizard", key="step2_restart"):
            env_vars = st.session_state.get("env_vars", {})
            env_vars_list = st.session_state.get("env_vars_list", [])
            #print(f"env_vars={env_vars} env_vars_list={env_vars_list}")
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.env_vars = env_vars
            st.session_state.env_vars_list = env_vars_list
            st.rerun()

# ----------------------------------------
# Step 2 (Batch): View/Download Listing
# ----------------------------------------
elif st.session_state.step == 2 and st.session_state.mode == "batch":
    st.header("Step 2: DAG Listing")
    for fname, data in st.session_state.batch_dags.items():
        col1, col2, col3 = st.columns([4, 1, 1])
        col1.markdown(f"**{fname}**  →  `{data['dag_id']}`")
        with col2:
            if st.button("👁 View", key=f"view_{fname}"):
                st.session_state.selected_dag_to_view = fname
                st.session_state.step = 3
                st.rerun()
        with col3:
            st.download_button("⬇ Download", data=data["code"], file_name=f"{data['dag_id']}.py", mime="text/x-python")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for fname, data in st.session_state.batch_dags.items():
            zf.writestr(f"{data['dag_id']}.py", data["code"])
    st.download_button("📦 Download All DAGs as ZIP", data=buf.getvalue(), file_name="all_dags.zip", mime="application/zip")
    
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step2_back"):
            st.session_state.step = 0
            st.rerun()
    with cols[1]:
        if st.button("🔄 Restart Wizard", key="step2_restart"):
            env_vars = st.session_state.get("env_vars", {})
            env_vars_list = st.session_state.get("env_vars_list", [])
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.env_vars = env_vars
            st.session_state.env_vars_list = env_vars_list
            st.rerun()

# ----------------------------------------
# Step 3: View Selected DAG (Batch Mode)
# ----------------------------------------
elif st.session_state.step == 3 and st.session_state.mode == "batch":
    fname = st.session_state.selected_dag_to_view
    dag_info = st.session_state.batch_dags[fname]
    st.subheader(f"👁 Viewing DAG for: `{fname}`")
    show_summary_and_dag(dag_info["jobs_dict"])
    st.subheader("📝 DAG Code")
    st_ace(value=dag_info["code"], language="python", key=f"view_dag_{fname}")
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step3_back"):
            st.session_state.step = 2
            st.rerun()
    with cols[1]:
        if st.button("🔄 Restart Wizard", key="step3_restart"):
            env_vars = st.session_state.get("env_vars", {})
            env_vars_list = st.session_state.get("env_vars_list", [])
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.env_vars = env_vars
            st.session_state.env_vars_list = env_vars_list
            st.rerun()
# ----------------------------------------
# Step -1: External dependency handling in batch mode
# ----------------------------------------
elif st.session_state.step == -1 and st.session_state.mode == "batch":
    st.error("⚠️ External dependency found!")
    for fname, ext_dep_list in st.session_state.ext_dep_dict.items():
        st.write(f"External dependency found: {ext_dep_list} in file {fname}")
    st.error("⚠️ Remove files having external dependency!")
    if st.button("🔄 Restart Wizard", key="step_minus1_restart"):
        env_vars = st.session_state.get("env_vars", {})
        env_vars_list = st.session_state.get("env_vars_list", [])
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.session_state.env_vars = env_vars
        st.session_state.env_vars_list = env_vars_list
        st.rerun()
# ----------------------------------------
# Step 9: External dependency handling in single mode
# ----------------------------------------
elif st.session_state.step == 9 and st.session_state.mode == "single":
    st.error("⚠️ External dependency found!")
    for fname, ext_dep_list in st.session_state.ext_dep_dict.items():
        st.write(f"External dependency found: {ext_dep_list} in file {fname}")
    option = st.radio("Upload required JIL files", ["Merge - This will generate single Dag", "Separate - This will generate separate Dags"])
    st.session_state.ext_option = "merge" if "Merge" in option else "separate"
    ext_files = st.file_uploader("Upload JIL files", type=["jil", "txt"], accept_multiple_files=True, key="step9_file_uploader")
    st.subheader("Already uploaded files:")
    for fname in st.session_state.jil_files_full_name:
        st.write(f"- {fname}")
    if ext_files:
        st.session_state.jil_files.extend([os.path.splitext(file.name)[0] for file in ext_files]) #Appending new files
        st.session_state.jil_files_full_name.extend([file.name for file in ext_files])
        if not isinstance(ext_files, list):
            ext_files = [ext_files]
        parsed_files = {}
        parser = None
        if st.session_state.ext_option == "merge":
            parser = JILParser()
        for f in ext_files:
            try:
                if st.session_state.ext_option == "separate":
                    parser = JILParser()
                content = f.read().decode("utf-8")
                jobs = parser.parse_content(content)
                # Always pass env vars as job.envvars
                for job in jobs.values():
                    if hasattr(job, "command") and job.command:
                        if hasattr(job, "envvars") and isinstance(job.envvars, dict):
                            #print(f"current job.envvars={job.envvars}")
                            job.envvars.update(st.session_state.env_vars)
                            #print(f"update job.envvars={st.session_state.env_vars}")
                        else:
                            #print(f"current job.envvars={job.envvars}")
                            job.envvars = dict(st.session_state.env_vars)
                            #print(f"add job.envvars={st.session_state.env_vars}")
                # Substitute in command if checked
                if st.session_state.get("substitute_in_command", False):
                    for job in jobs.values():
                        if hasattr(job, "command") and job.command:
                            job.command = substitute_env_vars(job.command, st.session_state.env_vars)
                        if hasattr(job, "watch_file") and job.watch_file:
                            job.watch_file = substitute_env_vars(job.watch_file, st.session_state.env_vars)
                if not jobs:
                    st.warning(f"⚠️ No jobs found in: {f.name}")
                    continue
                # ----------------------------------------
                # Start of validation
                # if mode is batch then check in each file for external dependency. If present mark as failure
                # ----------------------------------------
                #ext_dep_to_job_map = ExternalDepUtils.extract_external_dependency_to_job_mapping(jobs)
                #if st.session_state.ext_option == "separate" and ext_dep_to_job_map:
                    #st.session_state.validation_failed = True
                    #st.session_state.validation_msg = f"⚠️ Job definition not found for '{' , '.join(ext_dep_to_job_map.keys())}' in JIL: '{f.name}'. \n\n Remove '{f.name}' from selection."
                    #st.session_state.step = -11
                    #st.rerun()
                # ----------------------------------------
                # End of Validation
                # ----------------------------------------                    
                parsed_files[f.name] = jobs
            except Exception as e:
                st.error(f"❌ Error parsing {f.name}: {e}")
        if parsed_files:
            if st.session_state.ext_option == "merge":
                st.session_state.jil_content = content
                for job_dict in parsed_files.values():
                    st.session_state.jobs_dict.update(job_dict)
                # ----------------------------------------
                # Start of validation
                # Check for external dependency. If present mark as failure
                # ----------------------------------------
                #ext_dep_to_job_map = ExternalDepUtils.extract_external_dependency_to_job_mapping(st.session_state.jobs_dict)
                #if ext_dep_to_job_map:
                    #st.session_state.validation_failed = True
                    #st.session_state.validation_msg =f"⚠️ Job definition not found for '{' , '.join(ext_dep_to_job_map.keys())}' in JIL: '{' + '.join(parsed_files.keys())}'"
                    #st.session_state.step = 99
                    #st.rerun()                
            else:
                for file_key, jobs_dict in parsed_files.items():
                    if file_key in st.session_state.batch_jobs_dicts:
                        st.session_state.batch_jobs_dicts[file_key].update(jobs_dict)
                    else:
                        st.session_state.batch_jobs_dicts[file_key] = jobs_dict
            cols = st.columns([3, 1, 3])
            with cols[0]:
                if st.button("Next ➡️", key="step9_next"):
                    st.session_state.step = 1
                    if st.session_state.ext_option == "separate":
                        st.session_state.mode = "batch"
                    st.rerun()
            with cols[1]:
                if st.button("🔄 Restart Wizard", key="step9_restart"):
                    env_vars = st.session_state.get("env_vars", {})
                    env_vars_list = st.session_state.get("env_vars_list", [])
                    #print(f"env_vars={env_vars} env_vars_list={env_vars_list}")
                    for key in list(st.session_state.keys()):
                        del st.session_state[key]
                    st.session_state.env_vars = env_vars
                    st.session_state.env_vars_list = env_vars_list
                    st.rerun()
    else:
        cols = st.columns([3, 1, 3])
        with cols[0]:
            st.button("Next ➡️", key="step0_next_nofile", disabled=True)