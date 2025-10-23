import os
from pathlib import Path
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.ssh.hooks.ssh import SSHHook
import re

class ProfileAwareSftpSensor(SFTPSensor):
    """
    SFTPSensor that loads environment variables from a profile file
    and expands them in the path before monitoring.
    """

    def __init__(self, path, profile_file=None, *args, **kwargs):
        super().__init__(path=path, *args, **kwargs)
        self.profile_file = profile_file

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
                cmd = f"bash -lic 'set -o allexport; source {self.profile_file} >/dev/null 2>&1; env 2>/dev/null'"
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

    def poke(self, context):
        env = self._load_profile_env()
        self.path = self._expand_path(env)
        self.log.info("Monitoring file: %s", self.path)
        return super().poke(context)