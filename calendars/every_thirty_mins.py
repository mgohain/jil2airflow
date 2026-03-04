from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from airflow.utils.timezone import convert_to_utc
from dateutil.relativedelta import relativedelta
from typing import Optional, Any
import pendulum
from pendulum import Time


class EveryThirtyMinutesTimetable(Timetable):
    """
    Timetable to run DAGs every 30 minutes.

    - Scheduled runs: every 30 minutes aligned to clock
    - Data interval: previous 30-min window → current window
    - Manual runs: trigger time → next aligned window
    """

    def __init__(self, timezone: str = "UTC"):
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

        if last_automated_data_interval:
            # Move safely past last interval boundary
            candidate = last_automated_data_interval.end + relativedelta(seconds=1)
        else:
            candidate = restriction.earliest or pendulum.now(self.timezone)

        next_run = self._align_to_next_30_min(candidate)

        # catchup=False → skip past intervals
        if not restriction.catchup:
            now_local = pendulum.now(self.timezone)
            if next_run < now_local:
                next_run = self._align_to_next_30_min(now_local)

        # end_date / restriction.latest
        if restriction.latest and next_run > restriction.latest:
            return None

        prev_run = next_run - relativedelta(minutes=30)

        return DagRunInfo(
            run_after=next_run,
            data_interval=DataInterval(
                start=prev_run,
                end=next_run,
            ),
        )

    # ------------------------
    # Manual DAG runs
    # ------------------------
    def infer_manual_data_interval(self, run_after):
        """
        Manual trigger:
        trigger time → next aligned 30-minute boundary
        """
        if run_after is None:
            run_after = pendulum.now(self.timezone)
        else:
            run_after = run_after.in_timezone(self.timezone)

        next_run = self._align_to_next_30_min(run_after)

        return DataInterval(
            start=convert_to_utc(run_after),
            end=convert_to_utc(next_run),
        )

    # ------------------------
    # Helper: align to next 30 min
    # ------------------------
    def _align_to_next_30_min(self, dt):
        """
        Aligns datetime to the next 30-minute boundary.
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=self.timezone)
        else:
            dt = dt.in_timezone(self.timezone)

        minute = dt.minute
        next_minute = 30 if minute < 30 else 60

        aligned = dt.replace(
            minute=0 if next_minute == 60 else next_minute,
            second=0,
            microsecond=0,
        )

        if next_minute == 60:
            aligned = aligned + relativedelta(hours=1)

        if dt >= aligned:
            aligned = aligned + relativedelta(minutes=30)

        return aligned
    #def serialize(self) -> dict[str, Any]:
        #return {"schedule_at": self._schedule_at.isoformat()}

    #@classmethod
    #def deserialize(cls, value: dict[str, Any]) -> Timetable:
        #return cls(Time.fromisoformat(value["schedule_at"]))


class EveryThirtyMinutesTimetablePlugin(AirflowPlugin):
    name = "every_thirty_minutes_timetable_plugin"
    timetables = [EveryThirtyMinutesTimetable]