import re
from typing import List, Tuple

class ConditionParser:
    @staticmethod
    def normalize_condition(condition: str) -> str:
        print(condition)
        # Normalize short forms and long forms to canonical lowercase long forms (case insensitive)
        condition = re.sub(r'\b[sS]\(', 'success(', condition)
        condition = re.sub(r'\bSUCCESS\(', 'success(', condition, flags=re.IGNORECASE)
        condition = re.sub(r'\b[fF]\(', 'failure(', condition)
        condition = re.sub(r'\bFAILURE\(', 'failure(', condition, flags=re.IGNORECASE)
        condition = re.sub(r'\b[dD]\(', 'done(', condition)
        condition = re.sub(r'\bDONE\(', 'done(', condition, flags=re.IGNORECASE)
        condition = re.sub(r'\b[nN]\(', 'notrunning(', condition)
        condition = re.sub(r'\bNOTRUNNING\(', 'notrunning(', condition, flags=re.IGNORECASE)
        condition = re.sub(r'\b[tT]\(', 'terminated(', condition)
        condition = re.sub(r'\bTERMINATED\(', 'terminated(', condition, flags=re.IGNORECASE)
        # Logical operators are case insensitive now
        condition = re.sub(r'\b(and|AND)\b', r' & ', condition)
        condition = re.sub(r'\b(or|OR)\b', r' | ', condition)
        condition = re.sub(r'(?<!\s)&(?!\s)', r' & ', condition)
        condition = re.sub(r'(?<!\s)\|(?!\s)', r' | ', condition)
        condition = re.sub(r'(?<!!)=', '==', condition)
        condition = re.sub(r'\s{2,}', ' ', condition).strip()
        print(f"Normalized Condition: {condition}")
        return condition

    @staticmethod
    def parse_condition(condition: str) -> Tuple[str, List[str]]:
        if not condition:
            return "none", []
        condition = condition.strip()
        normalized_condition = ConditionParser.normalize_condition(condition)
        # Updated pattern to match both regular jobs and box jobs
        job_pattern = r'(?:success|failure|done|notrunning|terminated|exitcode)\(([a-zA-Z_][a-zA-Z0-9_]*)\)'
        dependencies = list(set(re.findall(job_pattern, normalized_condition)))
        print(f"Dependencies found: {dependencies}")
        # Same conditions apply for both regular jobs and box jobs
        if re.match(r'^success\([^)]+\)$', normalized_condition):
            return "all_success", dependencies
        elif re.match(r'^failure\([^)]+\)$', normalized_condition):
            return "all_failed", dependencies
        elif re.match(r'^done\([^)]+\)$', normalized_condition):
            return "all_done", dependencies
        elif ' & ' in normalized_condition and not ('|' in normalized_condition or '!' in normalized_condition):
            if all('success(' in part for part in normalized_condition.split(' & ')):
                return "all_success", dependencies
            else:
                return "complex", dependencies
        elif ' | ' in normalized_condition and not ('&' in normalized_condition or '!' in normalized_condition):
            if all('success(' in part for part in normalized_condition.split(' | ')):
                return "one_success", dependencies
            else:
                return "complex", dependencies
        else:
            return "complex", dependencies

    @staticmethod
    def generate_condition_code(condition: str, dependencies: List[str]) -> str:
        python_condition = ConditionParser.normalize_condition(condition)
        for dep in dependencies:
            python_condition = python_condition.replace(
                f'success({dep})', f"task_instance.xcom_pull(task_ids='{dep}', key='status') == 'success'"
            )
            python_condition = python_condition.replace(
                f'failure({dep})', f"task_instance.xcom_pull(task_ids='{dep}', key='status') == 'failed'"
            )
            python_condition = python_condition.replace(
                f'done({dep})', f"task_instance.xcom_pull(task_ids='{dep}', key='status') in ['success', 'failed']"
            )
            python_condition = python_condition.replace(
                f'exitcode({dep})', f"task_instance.xcom_pull(task_ids='{dep}', key='return_value')"
            )
        python_condition = re.sub(r'!(?!=)', 'not ', python_condition)
        python_condition = python_condition.replace(' & ', ' and ')
        python_condition = python_condition.replace(' | ', ' or ')
        return python_condition