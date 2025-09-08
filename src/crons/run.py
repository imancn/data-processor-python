import sys
import asyncio
from crons.registry import JOBS, backfill


async def _run(job_name: str, action: str = "run", **kwargs) -> int:
    if job_name not in JOBS:
        print(f"Unknown job: {job_name}")
        return 1
    if action == "run":
        return await JOBS[job_name]()
    if action == "backfill":
        hours = int(kwargs.get('hours', 0))
        return await backfill(job_name, hours)
    print(f"Unknown action: {action}")
    return 1


def main():
    if len(sys.argv) < 2:
        print("Usage: python -m crons.run <job_name> [backfill <hours>]")
        print("Available jobs:", ", ".join(sorted(JOBS.keys())))
        sys.exit(1)
    job_name = sys.argv[1]
    if len(sys.argv) >= 3 and sys.argv[2] == 'backfill':
        hours = int(sys.argv[3]) if len(sys.argv) >= 4 else 0
        exit_code = asyncio.run(_run(job_name, action='backfill', hours=hours))
    else:
        exit_code = asyncio.run(_run(job_name))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()


