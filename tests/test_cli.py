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
