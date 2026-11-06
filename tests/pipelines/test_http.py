import pytest
from conftest import Reply
from transitiq_pipelines.http import HttpClient, HttpError, RetryPolicy, build_url, check_scheme


def test_retries_503_then_succeeds(fake_server, no_sleep_http):
    fake_server.add("/x", Reply(503), Reply(503), Reply(200, b"ok"))
    resp = no_sleep_http.get(fake_server.base_url + "/x")
    assert resp.body == b"ok"
    assert len(fake_server.hits("/x")) == 3
    assert len(no_sleep_http.sleeps) == 2


def test_404_is_not_retried(fake_server, no_sleep_http):
    fake_server.add("/missing", Reply(404))
    with pytest.raises(HttpError) as err:
        no_sleep_http.get(fake_server.base_url + "/missing")
    assert err.value.status == 404
    assert len(fake_server.hits("/missing")) == 1


def test_gives_up_after_max_attempts(fake_server, no_sleep_http):
    fake_server.add("/down", Reply(500))
    with pytest.raises(HttpError):
        no_sleep_http.get(fake_server.base_url + "/down")
    assert len(fake_server.hits("/down")) == 4


def test_retry_after_header_is_honoured(fake_server):
    sleeps = []
    client = HttpClient(policy=RetryPolicy(max_attempts=2, max_delay=60), sleep=sleeps.append)
    fake_server.add("/limited", Reply(429, headers={"Retry-After": "7"}), Reply(200, b"ok"))
    client.get(fake_server.base_url + "/limited")
    assert sleeps == [7.0]


def test_mbta_ratelimit_reset_is_honoured(fake_server):
    sleeps = []
    client = HttpClient(
        policy=RetryPolicy(max_attempts=2), sleep=sleeps.append, clock=lambda: 1000.0
    )
    fake_server.add("/v3", Reply(429, headers={"x-ratelimit-reset": "1010"}), Reply(200, b"{}"))
    client.get(fake_server.base_url + "/v3")
    assert sleeps == [11.0]  # 10 s until reset + 1 s margin


def test_download_is_atomic_and_hashed(fake_server, no_sleep_http, tmp_path):
    fake_server.add("/f", Reply(200, b"abc"))
    dest = tmp_path / "out.bin"
    result = no_sleep_http.download(fake_server.base_url + "/f", dest)
    assert dest.read_bytes() == b"abc"
    assert result["bytes"] == 3
    assert result["sha256"] == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert not (tmp_path / "out.bin.part").exists()


def test_failed_download_leaves_no_file(fake_server, no_sleep_http, tmp_path):
    fake_server.add("/f", Reply(500))
    dest = tmp_path / "out.bin"
    with pytest.raises(HttpError):
        no_sleep_http.download(fake_server.base_url + "/f", dest)
    assert not dest.exists()
    assert not (tmp_path / "out.bin.part").exists()


def test_params_keep_commas_and_brackets():
    url = build_url("https://api-v3.mbta.com/facilities", {"filter[type]": "ELEVATOR,ESCALATOR"})
    assert url == "https://api-v3.mbta.com/facilities?filter%5Btype%5D=ELEVATOR,ESCALATOR"


@pytest.mark.parametrize(
    "url", ["http://example.com/x", "ftp://example.com/x", "file:///etc/passwd"]
)
def test_non_https_refused(url):
    with pytest.raises(ValueError):
        check_scheme(url)


def test_user_agent_sent(fake_server, no_sleep_http):
    fake_server.add("/ua", Reply(200, b""))
    no_sleep_http.get(fake_server.base_url + "/ua")
    assert fake_server.hits("/ua")[0]["headers"]["user-agent"].startswith("TransitIQ/")
