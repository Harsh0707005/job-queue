#!/usr/bin/env python3
"""Create/upgrade Postgres schema then exit (used by load-go stack)."""
import os
import sys
from job_queue import JobQueue

dsn = os.environ.get("JOB_QUEUE_DSN") or os.environ.get("DATABASE_URL")
if not dsn:
    sys.exit("JOB_QUEUE_DSN required")
q = JobQueue.connect(dsn, min_size=1, max_size=2)
q.close()
print("schema ready", flush=True)
