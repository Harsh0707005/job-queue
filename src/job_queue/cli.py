"""Read-only CLI for inspecting tenant stats and pause flags."""

from __future__ import annotations

import argparse
import os
import sys
from typing import Sequence

from job_queue import JobQueue, TenantStats


def _resolve_dsn(explicit: str | None) -> str:
    dsn = explicit or os.environ.get("JOB_QUEUE_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit(
            "database DSN required: pass --dsn or set JOB_QUEUE_DSN / DATABASE_URL"
        )
    # Accept SQLAlchemy-style driver URLs if present.
    return dsn.replace("postgresql+psycopg://", "postgresql://", 1).replace(
        "postgresql+psycopg2://", "postgresql://", 1
    )


def _format_rate(rate: float | None, burst: int | None) -> str:
    if rate is None:
        return "unlimited"
    if burst is not None:
        return f"{rate:g}/s burst={burst}"
    return f"{rate:g}/s"


def _format_stats(stats: TenantStats) -> str:
    paused = "yes" if stats.paused else "no"
    limit = (
        str(stats.max_concurrent_leases)
        if stats.max_concurrent_leases is not None
        else "unlimited"
    )
    pending_cap = (
        str(stats.max_pending_jobs)
        if stats.max_pending_jobs is not None
        else "unlimited"
    )
    return (
        f"tenant={stats.tenant_id}\n"
        f"  paused={paused}\n"
        f"  max_concurrent_leases={limit}\n"
        f"  max_pending_jobs={pending_cap}\n"
        f"  enqueue_rate={_format_rate(stats.enqueue_rate_per_sec, stats.enqueue_burst)}\n"
        f"  lease_rate={_format_rate(stats.lease_rate_per_sec, stats.lease_burst)}\n"
        f"  pending={stats.pending}\n"
        f"  leased={stats.leased}\n"
        f"  claimable={stats.claimable}\n"
        f"  completed={stats.completed}\n"
        f"  failed={stats.failed}\n"
        f"  dead={stats.dead}\n"
        f"  cancelled={stats.cancelled}\n"
        f"  total={stats.total}"
    )


def _format_table(rows: Sequence[TenantStats]) -> str:
    if not rows:
        return "(no tenants)"

    headers = (
        "TENANT",
        "PAUSED",
        "LIMIT",
        "PENDING",
        "LEASED",
        "CLAIMABLE",
        "COMPLETED",
        "FAILED",
        "DEAD",
        "CANCELLED",
        "TOTAL",
    )
    table_rows: list[tuple[str, ...]] = [headers]
    for s in rows:
        limit = (
            str(s.max_concurrent_leases) if s.max_concurrent_leases is not None else "-"
        )
        table_rows.append(
            (
                s.tenant_id,
                "yes" if s.paused else "no",
                limit,
                str(s.pending),
                str(s.leased),
                str(s.claimable),
                str(s.completed),
                str(s.failed),
                str(s.dead),
                str(s.cancelled),
                str(s.total),
            )
        )

    widths = [max(len(row[i]) for row in table_rows) for i in range(len(headers))]
    lines: list[str] = []
    for idx, row in enumerate(table_rows):
        line = "  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row))
        lines.append(line)
        if idx == 0:
            lines.append("  ".join("-" * widths[i] for i in range(len(headers))))
    return "\n".join(lines)


def cmd_stats(args: argparse.Namespace) -> int:
    dsn = _resolve_dsn(args.dsn)
    queue = JobQueue.connect(dsn, min_size=1, max_size=2)
    try:
        if args.tenant:
            stats = queue.get_tenant_stats(args.tenant)
            print(_format_stats(stats))
        else:
            all_stats = queue.list_tenant_stats()
            if args.verbose:
                if not all_stats:
                    print("(no tenants)")
                else:
                    print("\n\n".join(_format_stats(s) for s in all_stats))
            else:
                print(_format_table(all_stats))
    finally:
        queue.close()
    return 0


def cmd_offboard(args: argparse.Namespace) -> int:
    dsn = _resolve_dsn(args.dsn)
    queue = JobQueue.connect(dsn, min_size=1, max_size=2)
    try:
        result = queue.offboard_tenant(args.tenant)
        print(
            f"tenant={result.tenant_id}\n"
            f"  pending_cancelled={result.pending_cancelled}\n"
            f"  schedules_deleted={result.schedules_deleted}\n"
            f"  webhooks_deleted={result.webhooks_deleted}\n"
            f"  jobs_purged={result.jobs_purged}\n"
            f"  leased_remaining={result.leased_remaining}\n"
            f"  webhooks_delivering_remaining="
            f"{result.webhooks_delivering_remaining}\n"
            f"  offboarding={result.offboarding}\n"
            f"  empty={result.empty}"
        )
    finally:
        queue.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="job-queue",
        description="Inspection and admin tools for the multi-tenant job queue.",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="Postgres DSN (default: JOB_QUEUE_DSN or DATABASE_URL)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    stats = sub.add_parser(
        "stats",
        help="Print per-tenant job counts and pause flags (read-only).",
    )
    stats.add_argument(
        "--tenant",
        "-t",
        default=None,
        help="Show a single tenant (default: all tenants with jobs/settings)",
    )
    stats.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="When listing all tenants, print multi-line blocks instead of a table",
    )
    stats.set_defaults(func=cmd_stats)

    offboard = sub.add_parser(
        "offboard",
        help=(
            "Fully offboard a tenant: cancel pending work, delete schedules "
            "and webhooks, purge history; in-flight leases may finish then "
            "the tenant is auto-emptied."
        ),
    )
    offboard.add_argument(
        "--tenant",
        "-t",
        required=True,
        help="Tenant id to offboard",
    )
    offboard.set_defaults(func=cmd_offboard)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
