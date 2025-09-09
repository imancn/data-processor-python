import os
import shutil
import subprocess


def install(job_name: str = "cmc_hourly_prices") -> None:
    # __file__ = <project_root>/src/crons/install.py → project_root = dirname(dirname(dirname(__file__)))
    project_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    logs_dir = os.path.join(project_dir, 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    venv_python = os.path.join(project_dir, '.venv', 'bin', 'python')
    if not os.path.exists(venv_python):
        venv_python = 'python3'
    if shutil.which('crontab') is None:
        raise RuntimeError("crontab command not found on host; install cron (cronie) first")
    cron_line = f"0 * * * * cd {project_dir}/src && {venv_python} -m crons.run {job_name} >> {logs_dir}/cron.log 2>&1"
    existing = subprocess.run(['crontab', '-l'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    current_lines = []
    if existing.returncode == 0:
        current_lines = [l for l in existing.stdout.splitlines() if 'crons.run' not in l]
    current_lines.append(cron_line)
    new_cron = "\n".join(current_lines) + "\n"
    applied = subprocess.run(['crontab', '-'], input=new_cron, text=True)
    if applied.returncode != 0:
        raise RuntimeError(f"Failed to install crontab: exit {applied.returncode}")
    verify = subprocess.run(['crontab', '-l'], stdout=subprocess.PIPE, text=True)
    print(verify.stdout)


def main():
    import sys
    job = sys.argv[1] if len(sys.argv) > 1 else 'cmc_hourly_prices'
    install(job)


if __name__ == '__main__':
    main()


