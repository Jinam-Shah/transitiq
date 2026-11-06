import pytest
from transitiq_pipelines import fsutil


def test_replace_retries_on_windows_lock(tmp_path, monkeypatch):
    src, dest = tmp_path / "a.tmp", tmp_path / "a"
    src.write_text("new")
    real_replace = fsutil.os.replace
    calls = []

    def flaky(a, b):
        calls.append(1)
        if len(calls) < 3:
            raise PermissionError("[WinError 5] Access is denied")
        real_replace(a, b)

    monkeypatch.setattr(fsutil.os, "replace", flaky)
    sleeps = []
    fsutil.atomic_replace(src, dest, sleep=sleeps.append)
    assert dest.read_text() == "new"
    assert len(calls) == 3 and len(sleeps) == 2


def test_replace_gives_up_eventually(tmp_path, monkeypatch):
    def always_locked(a, b):
        raise PermissionError("locked")

    monkeypatch.setattr(fsutil.os, "replace", always_locked)
    with pytest.raises(PermissionError):
        fsutil.atomic_replace(tmp_path / "a", tmp_path / "b", attempts=3, sleep=lambda s: None)
