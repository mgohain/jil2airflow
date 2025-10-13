from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import Timetable


class AfterWorkdayTimetable(Timetable):
    pass


class WorkdayTimetablePlugin(AirflowPlugin):
    name = "workday_timetable_plugin"
    timetables = [AfterWorkdayTimetable]

class weekday_calendar(Timetable):
    pass