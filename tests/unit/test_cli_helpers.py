import pytest

from src.cli import app


def test_ensure_list_handles_none_and_iterables():
    assert app._ensure_list(None) == []
    assert app._ensure_list(["a"]) == ["a"]
    assert app._ensure_list(("a", "b")) == ["a", "b"]


def test_inject_config_arg_adds_when_missing():
    assert app._inject_config_arg([], "config.yml") == ["--config", "config.yml"]
    assert app._inject_config_arg(["--foo"], "config.yml") == [
        "--config",
        "config.yml",
        "--foo",
    ]


def test_inject_config_arg_skips_when_present():
    args = ["--config", "custom.yml", "--foo"]
    assert app._inject_config_arg(args, "config.yml") == args
    args = ["--config=custom.yml", "--foo"]
    assert app._inject_config_arg(args, "config.yml") == args


def test_extract_arg_value_parses_flags():
    assert app._extract_arg_value(["--date", "2024-01-01"], "date") == "2024-01-01"
    assert app._extract_arg_value(["--date=2024-01-02"], "date") == "2024-01-02"
    assert app._extract_arg_value(["--other", "x"], "date") is None


def test_compose_base_cmd_prefers_docker_compose(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(app.shutil, "which", lambda name: "/bin/docker-compose")
    assert app._compose_base_cmd() == ["docker-compose"]


def test_compose_base_cmd_falls_back_to_docker(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(app.shutil, "which", lambda name: None)
    assert app._compose_base_cmd() == ["docker", "compose"]


def test_run_cmd_sets_env(monkeypatch: pytest.MonkeyPatch):
    captured = {}

    class Result:
        returncode = 0

    def fake_run(cmd, env=None, cwd=None):
        captured["cmd"] = cmd
        captured["env"] = env or {}
        captured["cwd"] = cwd
        return Result()

    monkeypatch.setattr(app.subprocess, "run", fake_run)
    app._run_cmd(["echo", "ok"], env_overrides={"X": "1"})
    assert captured["cmd"] == ["echo", "ok"]
    assert captured["env"]["X"] == "1"
    assert captured["env"]["ECOM_CLI_SUPPRESS_DEPRECATION"] == "1"
    assert captured["cwd"] is None


def test_run_python_helpers_delegate(monkeypatch: pytest.MonkeyPatch):
    calls = []

    def fake_run_cmd(cmd, env_overrides=None, cwd=None, check=True):
        calls.append((cmd, env_overrides, cwd, check))
        return 0

    monkeypatch.setattr(app, "_run_cmd", fake_run_cmd)
    app._run_python_script("script.py", ["--flag"], env_overrides={"A": "1"})
    app._run_python_module("mod", ["--x"], env_overrides={"B": "2"})
    assert calls[0][0][:2] == [app.sys.executable, "script.py"]
    assert calls[1][0][:3] == [app.sys.executable, "-m", "mod"]


def test_demo_full_uses_defaults(monkeypatch: pytest.MonkeyPatch):
    captured = {}

    def fake_callback(*, demo_date, demo_end_date, lookback, dates):
        captured["demo_date"] = demo_date
        captured["demo_end_date"] = demo_end_date
        captured["lookback"] = lookback
        captured["dates"] = dates

    monkeypatch.setattr(app.local_demo, "callback", fake_callback)

    app.local_demo_full.callback()

    assert captured == {
        "demo_date": app._DEMO_DEFAULT_DATE,
        "demo_end_date": app._DEMO_DEFAULT_END_DATE,
        "lookback": app._DEMO_DEFAULT_LOOKBACK,
        "dates": app._DEMO_DEFAULT_DATES,
    }
