from dataclasses import dataclass, field
from typing import List, Dict, Any
import traceback

@dataclass
class AutosysJob:  
    """Represents an Autosys job with all its properties"""
    name: str
    job_type: str = "CMD"  # CMD, BOX, FW (file watcher)
    command: str = ""
    machine: str = ""
    owner: str = ""
    permission: str = ""
    date_conditions: int = 0
    days_of_week: List[str] = field(default_factory=list)
    start_times: List[str] = field(default_factory=list)
    condition: str = ""
    description: str = ""
    std_out_file: str = ""
    std_err_file: str = ""
    profile: str = ""
    envvars: dict = field(default_factory=dict)
    priority: int = 0
    max_run_alarm: int = 0
    min_run_alarm: int = 0
    watch_file: str = ""
    watch_file_min_size: int = 0
    box_name: str = ""
    success_codes: List[int] = field(default_factory=lambda: [0])
    failure_codes: List[int] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    command_image: str = ""
    fs_conn_id: str = ""  # Default connection ID for file sensors
    resource_requests: str = ""
    resource_limits: str = ""
    ssh_conn_id: str = ""
    operator_type: str = "KubernetesPodOperator"  # Default operator type
    namespace: str = ""
    run_calendar: str = ""
    watch_interval: int = 60
    start_mins: str = ""
    n_retrys: int = 0 # Number of retries for the job
    term_run_time: int = 0 #  If the job runs longer than the specified time, AutoSys Workload Automation terminates it
    alarm_if_fail: int = 0 # 0 or n | 1 or y
    alarm_if_terminated: int = 0 # 0 or n | 1 or y
    timezone: str = None # Job schedule timezone
    send_notification: int = 0 #specifies whether to send a notification when the job completes successfully, fails, or terminates or when an alarm is raised
    notification_emailaddress: str = ""
    notification_emailaddress_on_alarm:str = ""
    notification_emailaddress_on_failure: str = ""
    notification_emailaddress_on_success: str = ""
    notification_emailaddress_on_terminated: str = ""
    db_conn_id:str = ""
    sql_command: str = ""
    sp_name: str = ""
    sp_params: Dict[str, Any] = field(default_factory=dict)
    


    def is_file_watcher(self) -> bool:
        return self.job_type in ["f", "FW"] or bool(self.watch_file)

    def is_box_job(self) -> bool:
        return self.job_type in ["b", "box", "BOX"]

    def has_command(self) -> bool:
        return self.job_type in ["c", "CMD", "SQL", "sql", "DBPROC", "dbproc"] or bool(self.command)
    def is_sql_job(self) -> bool:
        return self.job_type in ["SQL", "sql", "DBPROC", "dbproc"]
    def has_sp_params(self) -> bool:
        """Return True if the job has stored procedure parameters."""
        return bool(self.sp_params)