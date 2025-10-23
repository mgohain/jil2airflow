import re
from typing import Dict
from autosys_job import AutosysJob
from collections import defaultdict
from condition_parser import ConditionParser

class ExternalDepUtils:
    @staticmethod
    def extract_external_dependency_to_job_mapping(jobs: Dict[str, AutosysJob]) -> dict:
        """
        Return a mapping of external_job -> list of jobs that depend on it.
        """
        dep_pattern = re.compile(
            r'(success|failure|done|notrunning|terminated|exitcode)\(([^)]+)\)',
            re.IGNORECASE,
        )

        defined_jobs = {job.name for job in jobs.values()}

        external_to_jobs = defaultdict(list)
        for job in jobs.values():
            if not job.condition:
                continue
            norm_condition = ConditionParser.normalize_condition(job.condition)
            for _, dep_job in dep_pattern.findall(norm_condition):
                dep_job = dep_job.strip()
                if dep_job not in defined_jobs:
                    external_to_jobs[dep_job].append(job.name)

        print(f"external dependency :: {external_to_jobs}")
        return dict(external_to_jobs)
    @staticmethod    
    def generate_external_dependency_tasks(external_task_to_dependent_task_map: Dict[str, str], external_task_to_dag_id_map: Dict[str, str]) -> str:
        external_task_name_prefix = "wait_for_"
        for ext_task, dag_id in external_task_to_dag_id_map.items():
            print("--------------------------Debug-------------")
            if dag_id is None:
                raise ValueError(f"External dependency task '{ext_task}' not found in Airflow.")

            else:
                print(f"{ext_task} -> {dag_id}")
        ext_task_defs = "\n\n".join(
            ExternalDepUtils.generate_external_task_sensor(ext_task, dag_id, external_task_name_prefix)
            for ext_task, dag_id in external_task_to_dag_id_map.items()
        )
        dependency_lines = [
            f"{external_task_name_prefix}{upstream.split('.')[-1]} >> {downstream}"
            for upstream, downstream_list in external_task_to_dependent_task_map.items()
            for downstream in downstream_list
        ]
        # Join into one string with newlines
        dependency_string = "\n".join(dependency_lines)
        return ext_task_defs + "\n\n" + dependency_string
    
    @staticmethod
    def generate_external_task_sensor(external_task_id: str, external_dag_id: str, external_task_name_prefix: str, indent: int = 0) -> str:
        indent_str = " " * indent
        ext_task_id_without_prefix = external_task_id.split(".")[-1]
        task_def = f"""
{indent_str}{external_task_name_prefix}{ext_task_id_without_prefix} = ExternalTaskSensor(
{indent_str}    task_id = '{external_task_name_prefix}{ext_task_id_without_prefix}',          
{indent_str}    external_dag_id = '{external_dag_id}',         # DAG to wait for
{indent_str}    external_task_id = '{external_task_id}',       # Task to wait for 
{indent_str}    poke_interval = 30,                            # check every 30s
{indent_str}    timeout = 3600,                                # fail if not found in 60 min
{indent_str}    mode = 'poke',
{indent_str})
"""
        return task_def    