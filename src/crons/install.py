import os
import subprocess


def install(job_name: str = "cmc_hourly_prices") -> None:
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_dir = os.path.join(project_dir, 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    venv_python = os.path.join(project_dir, '.venv', 'bin', 'python')
    if not os.path.exists(venv_python):
        venv_python = 'python3'
    cron_line = f"0 * * * * cd {project_dir}/src && {venv_python} -m crons.run {job_name} >> {logs_dir}/cron.log 2>&1"
    existing = subprocess.run(['crontab', '-l'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    lines = [l for l in existing.stdout.splitlines() if 'crons.run' not in l]
    lines.append(cron_line)
    new_cron = "\n".join(lines) + "\n"
    subprocess.run(['crontab', '-'], input=new_cron, text=True, check=False)


def main():
    import sys
    job = sys.argv[1] if len(sys.argv) > 1 else 'cmc_hourly_prices'
    install(job)


if __name__ == '__main__':
    main()


