from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from airflow.utils.timezone import convert_to_utc
from dateutil.relativedelta import relativedelta
from typing import Optional, Any
import pendulum
from pendulum import Time


class CDW_EC_TENTH_DAY_OF_MONTH(Timetable):
    """
    Timetable to run DAGs on the 10th of each month at a specific hour/minute
    in a given timezone.

    - Scheduled runs: 10th of each month at scheduled hour/minute
    - Data interval: previous 10th → current 10th
    - Manual runs: from trigger time → next 10th
    - Respects catchup, start_date, end_date
    """

    def __init__(self, hour: int = 6, minute: int = 10, timezone: str = "US/Eastern"):
        self.hour = hour
        self.minute = minute
        self.timezone = pendulum.timezone(timezone)

    # ------------------------
    # Scheduled DAG runs
    # ------------------------
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction,
    ) -> Optional[DagRunInfo]:

        # Case 1: previous automated run exists
        if last_automated_data_interval is not None:
            candidate = last_automated_data_interval.end + relativedelta(seconds=1)
            next_run = self._get_next_10th(candidate)
        else:
            # Case 2: first run
            earliest = restriction.earliest or pendulum.now(self.timezone)
            next_run = self._get_next_10th(earliest)

        # Case 3: catchup=False → skip past intervals
        if not restriction.catchup:
            now_local = pendulum.now(self.timezone)
            if next_run < now_local:
                next_run = self._get_next_10th(now_local)

        # Case 4: restriction.latest
        if restriction.latest and next_run > restriction.latest:
            return None

        # ✅ Correct monthly data interval: previous 10th → current 10th
        prev_10th = next_run - relativedelta(months=1)

        return DagRunInfo(
            run_after=next_run,
            data_interval=DataInterval(
                start=prev_10th,
                end=next_run,
            ),
        )

    # ------------------------
    # Manual DAG runs
    # ------------------------
    def infer_manual_data_interval(self, run_after):
        """
        Manual trigger interval:
        from trigger time → next scheduled 10th
        """
        if run_after is None:
            run_after = pendulum.now(self.timezone)
        else:
            run_after = run_after.in_timezone(self.timezone)

        next_10th = self._get_next_10th(run_after)

        return DataInterval(
            start=convert_to_utc(run_after),
            end=convert_to_utc(next_10th),
        )

    # ------------------------
    # Helper: compute next 10th
    # ------------------------
    def _get_next_10th(self, dt):
        """
        Returns the next 10th of the month at scheduled hour/minute
        in timetable timezone.
        """
        if dt is None:
            dt = pendulum.now(self.timezone)
        else:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=self.timezone)
            else:
                dt = dt.in_timezone(self.timezone)

        year = dt.year
        month = dt.month

        day_10 = dt.replace(
            day=30,
            hour=self.hour,
            minute=self.minute,
            second=0,
            microsecond=0,
        )

        # If already past the 10th, move to next month
        if dt >= day_10:
            month += 1
            if month > 12:
                month = 1
                year += 1
            day_10 = day_10.replace(year=year, month=month, day=30)

        return day_10
    #def serialize(self) -> dict[str, Any]:
        #return {"schedule_at": self._schedule_at.isoformat()}

    #@classmethod
    #def deserialize(cls, value: dict[str, Any]) -> Timetable:
        #return cls(Time.fromisoformat(value["schedule_at"]))


class TenthDayOfMonthTimetablePlugin(AirflowPlugin):
    name = "tenth_day_of_month_timetable_plugin"
    timetables = [CDW_EC_TENTH_DAY_OF_MONTH]