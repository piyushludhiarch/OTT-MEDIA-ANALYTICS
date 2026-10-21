from pathlib import Path

from src.ingestion.s3_storage import download_file, list_objects, upload_file


class FakePaginator:
    def paginate(self, **kwargs):
        assert kwargs == {"Bucket": "test-bucket", "Prefix": "raw/"}
        return [
            {"Contents": [{"Key": "raw/a.csv", "Size": 12}]},
            {"Contents": [{"Key": "raw/b.csv", "Size": 34}]},
        ]


class FakeS3:
    def __init__(self):
        self.calls = []

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return FakePaginator()

    def upload_file(self, *args, **kwargs):
        self.calls.append(("upload", args, kwargs))

    def download_file(self, *args):
        self.calls.append(("download", args))


def test_list_objects_uses_pagination(capsys):
    count = list_objects(FakeS3(), "test-bucket", "raw/")

    assert count == 2
    assert "raw/a.csv\t12 bytes" in capsys.readouterr().out


def test_upload_file_sets_content_type(tmp_path: Path):
    source = tmp_path / "events.csv"
    source.write_text("event_id\n", encoding="utf-8")
    s3 = FakeS3()

    upload_file(s3, "test-bucket", source, "raw/events.csv")

    assert s3.calls == [
        (
            "upload",
            (str(source), "test-bucket", "raw/events.csv"),
            {"ExtraArgs": {"ContentType": "text/csv"}},
        )
    ]


def test_download_file_creates_destination_directory(tmp_path: Path):
    destination = tmp_path / "downloads" / "content.csv"
    s3 = FakeS3()

    download_file(s3, "test-bucket", "raw/content.csv", destination)

    assert destination.parent.is_dir()
    assert s3.calls == [
        ("download", ("test-bucket", "raw/content.csv", str(destination)))
    ]
