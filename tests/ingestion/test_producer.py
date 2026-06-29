import gzip
import json

import pytest

from dev_trends.ingestion.producer import extract_push_event, publish_file


def test_extract_push_event_returns_repo_key_and_raw_value() -> None:
    event = {"id": "1", "type": "PushEvent", "repo": {"name": "apache/airflow"}}
    raw_line = json.dumps(event).encode()

    result = extract_push_event(raw_line)

    assert result == (b"apache/airflow", raw_line)


def test_extract_push_event_skips_non_push_event() -> None:
    event = {"id": "2", "type": "WatchEvent", "repo": {"name": "dbt-labs/dbt-core"}}
    raw_line = json.dumps(event).encode()

    assert extract_push_event(raw_line) is None


def test_extract_push_event_raises_on_malformed_json() -> None:
    with pytest.raises(json.JSONDecodeError):
        extract_push_event(b"{not valid json")


def test_publish_file_counts_published_and_skips_malformed(mocker, tmp_path) -> None:
    lines = [
        json.dumps({"id": "1", "type": "PushEvent", "repo": {"name": "apache/spark"}}),
        json.dumps({"id": "2", "type": "WatchEvent", "repo": {"name": "apache/spark"}}),
        "{not valid json",
    ]
    gz_path = tmp_path / "2024-01-15-0.json.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        for line in lines:
            f.write(line + "\n")

    producer = mocker.MagicMock()
    published, malformed = publish_file(producer, "github.push.raw", gz_path)

    assert (published, malformed) == (1, 1)
    assert producer.produce.call_count == 1
