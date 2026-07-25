"""CLI smoke tests (new file; does not modify existing tests)."""

from __future__ import annotations

from job_queue.cli import build_parser, cmd_stats, main


def test_cli_stats_prints_tenant_and_pause(queue, capsys, postgres_url):
    queue.enqueue(tenant_id="cli-a", payload={"n": 1})
    queue.pause_tenant("cli-b")  # settings-only tenant appears in list

    # Point CLI at the same disposable Docker DB.
    rc = main(["--dsn", postgres_url, "stats", "--tenant", "cli-a"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "tenant=cli-a" in out
    assert "paused=no" in out
    assert "pending=" in out

    rc = main(["--dsn", postgres_url, "stats"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "cli-a" in out
    assert "PAUSED" in out  # table header


def test_cli_parser_stats_subcommand():
    parser = build_parser()
    args = parser.parse_args(["stats", "-t", "acme", "-v"])
    assert args.command == "stats"
    assert args.tenant == "acme"
    assert args.verbose is True
    assert args.func is cmd_stats


def test_cli_parser_offboard_subcommand():
    from job_queue.cli import cmd_offboard

    parser = build_parser()
    args = parser.parse_args(["offboard", "-t", "acme"])
    assert args.command == "offboard"
    assert args.tenant == "acme"
    assert args.func is cmd_offboard


def test_cli_offboard_prints_result(queue, capsys, postgres_url):
    queue.enqueue(tenant_id="cli-off", payload={"n": 1})
    queue.create_schedule(
        tenant_id="cli-off",
        interval_seconds=60,
        payload={"t": 1},
    )
    rc = main(["--dsn", postgres_url, "offboard", "--tenant", "cli-off"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "tenant=cli-off" in out
    assert "pending_cancelled=" in out
    assert "empty=True" in out
    assert queue.tenant_is_empty("cli-off") is True
