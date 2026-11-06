import json

from transitiq_pipelines import cli
from transitiq_pipelines.config import load_dotenv
from transitiq_pipelines.report import summarise
from transitiq_pipelines.storage import LocalBronzeStore


def test_report_summarises_sources(tmp_path):
    store = LocalBronzeStore(tmp_path)
    store.put_bytes(
        "source=lamp_subway_otp/service_date=2026-11-01/a.parquet", b"x", {"rows": 10, "bytes": 1}
    )
    store.put_bytes(
        "source=lamp_subway_otp/service_date=2026-11-02/b.parquet", b"x", {"rows": 5, "bytes": 1}
    )
    store.put_bytes(
        "source=open_meteo_hourly/month=2026-10/w.json", b"x", {"rows": 744, "bytes": 2}
    )
    summary = summarise(tmp_path)
    lamp = summary["sources"]["lamp_subway_otp"]
    assert (lamp["files"], lamp["rows"], lamp["distinct_days"]) == (2, 15, 2)
    assert (lamp["first_day"], lamp["last_day"]) == ("2026-11-01", "2026-11-02")
    assert summary["sources"]["open_meteo_hourly"]["months"] == ["2026-10"]
    assert summary["totals"]["rows"] == 759


def test_cli_report_json(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert cli.main(["--data-dir", str(tmp_path / "data"), "report", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["totals"]["files"] == 0


def test_cli_parser_defaults():
    args = cli.build_parser().parse_args(["backfill"])
    assert (args.end - args.start).days == 364
    assert args.store == "local"


def test_dotenv_does_not_override(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text("# comment\nMBTA_API_KEY=from_file\nOTHER='quoted'\n\nBROKEN\n")
    monkeypatch.setenv("MBTA_API_KEY", "from_env")
    monkeypatch.delenv("OTHER", raising=False)
    load_dotenv(env)
    import os

    assert os.environ["MBTA_API_KEY"] == "from_env"
    assert os.environ["OTHER"] == "quoted"
