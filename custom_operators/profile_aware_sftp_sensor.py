"""This module contains ProfileAwareSFTPSensor sensor. (sftp version 5.4.0)"""

from __future__ import annotations

import os
from pathlib import Path
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from paramiko.sftp import SFTP_NO_SUCH_FILE

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.triggers.sftp import SFTPTrigger
from airflow.providers.sftp.version_compat import BaseSensorOperator, PokeReturnValue
from airflow.utils.timezone import convert_to_utc, parse
from airflow.providers.ssh.hooks.ssh import SSHHook
import re
if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context


class ProfileAwareSFTPSensor(BaseSensorOperator):
    """
    Waits for a file or directory to be present on SFTP.

    :param path: Remote file or directory path
    :param file_pattern: The pattern that will be used to match the file (fnmatch format)
    :param sftp_conn_id: The connection to run the sensor against
    :param newer_than: DateTime for which the file or file path should be newer than, comparison is inclusive
    :param deferrable: If waiting for completion, whether to defer the task until done, default is ``False``.
    """

    template_fields: Sequence[str] = (
        "path",
        "file_pattern",
        "newer_than",
    )

    def __init__(
        self,
        *,
        path: str,
        file_pattern: str = "",
        profile_file: str = None,
        newer_than: datetime | str | None = None,
        sftp_conn_id: str = "sftp_default",
        python_callable: Callable | None = None,
        op_args: list | None = None,
        op_kwargs: dict[str, Any] | None = None,
        use_managed_conn: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.file_pattern = file_pattern
        self.profile_file = profile_file
        self.hook: SFTPHook | None = None
        self.sftp_conn_id = sftp_conn_id
        self.newer_than: datetime | str | None = newer_than
        self.use_managed_conn = use_managed_conn
        self.python_callable: Callable | None = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.deferrable = deferrable

    def _get_files(self) -> list[str]:
        files_from_pattern: list[str] = []
        files_found: list[str] = []

        if self.file_pattern:
            files_from_pattern = self.hook.get_files_by_pattern(self.path, self.file_pattern)  # type: ignore[union-attr]
            if files_from_pattern:
                actual_files_present = [
                    os.path.join(self.path, file_from_pattern) for file_from_pattern in files_from_pattern
                ]
            else:
                return files_found
        else:
            try:
                # If a file is present, it is the single element added to the actual_files_present list to be
                # processed. If the file is a directory, actual_file_present will be assigned an empty list,
                # since SFTPHook.isfile(...) returns False
                actual_files_present = [self.path] if self.hook.isfile(self.path) else []  # type: ignore[union-attr]
            except Exception as e:
                raise AirflowException from e

        if self.newer_than:
            for actual_file_present in actual_files_present:
                try:
                    mod_time = self.hook.get_mod_time(actual_file_present)  # type: ignore[union-attr]
                    self.log.info("Found File %s last modified: %s", actual_file_present, mod_time)
                except OSError as e:
                    if e.errno != SFTP_NO_SUCH_FILE:
                        raise AirflowException from e
                    continue

                if isinstance(self.newer_than, str):
                    self.newer_than = parse(self.newer_than)
                _mod_time = convert_to_utc(datetime.strptime(mod_time, "%Y%m%d%H%M%S"))
                _newer_than = convert_to_utc(self.newer_than)
                if _newer_than <= _mod_time:
                    files_found.append(actual_file_present)
                    self.log.info(
                        "File %s has modification time: '%s', which is newer than: '%s'",
                        actual_file_present,
                        str(_mod_time),
                        str(_newer_than),
                    )
                else:
                    self.log.info(
                        "File %s has modification time: '%s', which is older than: '%s'",
                        actual_file_present,
                        str(_mod_time),
                        str(_newer_than),
                    )
        else:
            files_found = actual_files_present

        return files_found
    def _load_profile_env(self):
        env = {}
        hook = SSHHook(ssh_conn_id=self.sftp_conn_id)  # <-- use same conn as your SFTP
        if not self.profile_file:
            #Loads environment variables from the remote host. Uses a login shell so that ~/.bashrc, ~/.profile, /etc/environment are sourced.

            #with hook.get_conn() as ssh_client:
            #    # Use login shell to source profile files
            #    cmd = "bash -lc 'env'"
            #    self.log.info("Running remote env command: %s", cmd)
            #    stdin, stdout, stderr = ssh_client.exec_command(cmd)
            #    output = stdout.read().decode().strip()
            #    error = stderr.read().decode().strip()
            #    if error:
            #        self.log.warning("Error while loading remote env: %s", error)
            # Parse the output into a dictionary
            #for line in output.splitlines():
            #    if "=" in line:
            #        k, v = line.split("=", 1)
            #        env[k] = v

            #self.log.info("Remote environment: %s", env)
            return env
        else:    
            # Run a shell to source the profile and dump env
            self.log.info("Profile: %s", self.profile_file)
            with hook.get_conn() as ssh_client:
                cmd = f"bash -lic 'set -o allexport; source {self.profile_file} >/dev/null 2>&1; env' 2>/dev/null"
                self.log.info("Running remote command: %s", cmd)
                stdin, stdout, stderr = ssh_client.exec_command(cmd)
                output = stdout.read().decode().strip()
                error = stderr.read().decode().strip()
                if error:
                    self.log.warning(error)
            #Parse the output into a dictionary
            for line in output.splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    env[k] = v
            #self.log.info("Remote environment: %s", env)
            return env

    def _expand_path(self, env):
        """
        Replace $VAR or ${VAR} in the path using the provided env dictionary,
        then expand ~ to the user home directory.
        """
        expanded = self.path

        # Match $VAR or ${VAR}
        pattern = re.compile(r"\$\{?(\w+)\}?")

        def replace_var(match):
            var_name = match.group(1)
            return env.get(var_name, match.group(0))  # leave unchanged if not found

        expanded = pattern.sub(replace_var, expanded)
        expanded = str(Path(expanded).expanduser())
        return expanded

    def poke(self, context: Context) -> PokeReturnValue | bool:
        env = self._load_profile_env()
        self.path = self._expand_path(env)
        self.hook = SFTPHook(self.sftp_conn_id, use_managed_conn=self.use_managed_conn)

        self.log.info("Poking for %s, with pattern %s", self.path, self.file_pattern)

        if self.use_managed_conn:
            files_found = self._get_files()
        else:
            with self.hook.get_managed_conn():
                files_found = self._get_files()

        if not len(files_found):
            return False

        if self.python_callable is not None:
            if self.op_kwargs:
                self.op_kwargs["files_found"] = files_found
            callable_return = self.python_callable(*self.op_args, **self.op_kwargs)
            return PokeReturnValue(
                is_done=True,
                xcom_value={"files_found": files_found, "decorator_return_value": callable_return},
            )
        return True

    def execute(self, context: Context) -> Any:
        # Unlike other async sensors, we do not follow the pattern of calling the synchronous self.poke()
        # method before deferring here. This is due to the current limitations we have in the synchronous
        # SFTPHook methods. They are as follows:
        #
        # For file_pattern sensing, the hook implements list_directory() method which returns a list of
        # filenames only without the attributes like modified time which is required for the file_pattern
        # sensing when newer_than is supplied. This leads to intermittent failures potentially due to
        # throttling by the SFTP server as the hook makes multiple calls to the server to get the
        # attributes for each of the files in the directory.This limitation is resolved here by instead
        # calling the read_directory() method which returns a list of files along with their attributes
        # in a single call. We can add back the call to self.poke() before deferring once the above
        # limitations are resolved in the sync sensor.
        if self.deferrable:
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=SFTPTrigger(
                    path=self.path,
                    file_pattern=self.file_pattern,
                    sftp_conn_id=self.sftp_conn_id,
                    poke_interval=self.poke_interval,
                    newer_than=self.newer_than,
                ),
                method_name="execute_complete",
            )
        else:
            return super().execute(context=context)

    def execute_complete(self, context: dict[str, Any], event: Any = None) -> None:
        """
        Execute callback when the trigger fires; returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event is not None:
            if "status" in event and event["status"] == "error":
                raise AirflowException(event["message"])

            if "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
                self.log.info(event["message"])
                return None

        raise AirflowException("No event received in trigger callback")