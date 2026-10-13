from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import anyio

from fmkorea_scraper.dom import SEOUL
from fmkorea_scraper.application.media_enrichment import media_relative_path
from fmkorea_scraper.models.job import S3StorageConfig, SpiderRuntimeConfig, StorageConfig
from fmkorea_scraper.outputs.local import LocalOutputStore
from fmkorea_scraper.outputs.s3_output import S3OutputStore


class Missing(Exception):
    response = {"Error": {"Code": "404"}}


class FakeS3:
    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise Missing()
        return {"Metadata": self.objects[Key][1]}

    def put_object(self, Bucket, Key, Body, Metadata):
        self.objects[Key] = (Body, Metadata)

    def upload_file(self, path, bucket, key, ExtraArgs):
        self.objects[key] = (Path(path).read_bytes(), ExtraArgs["Metadata"])

    def list_objects_v2(self, Bucket, Prefix, **kwargs):
        return {"Contents": [{"Key": key} for key in self.objects if key.startswith(Prefix)]}

    def get_object(self, Bucket, Key):
        from io import BytesIO
        return {"Body": BytesIO(self.objects[Key][0])}


def test_media_layout_follows_post_type_and_order():
    assert media_relative_path({"board_id": "humor", "document_srl": 123},
        {"url": "https://www.fmkorea.com/files/photo.jpg?x=1",
         "media_type": "image", "source_block_order": 4}) == "media/humor/123/image/4-photo.jpg"


def test_local_commit_and_s3_segment_ack():
    async def exercise(tmp):
        now = datetime.now(SEOUL)
        item = {"site_id": "fmkorea", "board_id": "humor", "document_id_raw": "123", "document_srl": 123}
        local = LocalOutputStore(tmp, "job", "run", now)
        receipt = await local.append_post(item)
        assert receipt.durable and local.staging.exists()
        assert (await local.finalize()) == str(local.final)

        fake = FakeS3()
        storage = StorageConfig(output_mode="s3_compatible", s3=S3StorageConfig(endpoint_url="http://localhost:3900", bucket="test"),
            base_output_path=str(tmp), s3_flush_records=2)
        store = S3OutputStore(storage, SpiderRuntimeConfig(), "job", "s3run", now, fake)
        first = await store.append_post(item)
        assert not first.durable
        second_item = {**item, "document_id_raw": "124", "document_srl": 124}
        second = await store.append_post(second_item)
        assert second.durable and len(second.committed_keys) == 2
        assert len(fake.objects) == 1
        assert {("fmkorea", "humor", "123"), ("fmkorea", "humor", "124")} == store.recover_committed_keys()
        store.staging.unlink()
        recovered = S3OutputStore(storage, SpiderRuntimeConfig(), "job", "s3run", now, fake)
        assert recovered.recover_committed_keys() == {("fmkorea", "humor", "123"), ("fmkorea", "humor", "124")}
        assert len(list(recovered.read_staging())) == 2
        await store.finalize()
        assert any(key.endswith("manifest.json") for key in fake.objects)

    with TemporaryDirectory() as folder:
        anyio.run(exercise, Path(folder))


def test_s3_transient_upload_keeps_pending_until_ack():
    class FlakyS3(FakeS3):
        def __init__(self):
            super().__init__()
            self.fail_once = True

        def put_object(self, Bucket, Key, Body, Metadata):
            if self.fail_once:
                self.fail_once = False
                raise OSError("temporary S3 failure")
            super().put_object(Bucket, Key, Body, Metadata)

    async def exercise(tmp):
        fake = FlakyS3()
        storage = StorageConfig(output_mode="s3_compatible",
            s3=S3StorageConfig(endpoint_url="http://localhost:3900", bucket="test"),
            base_output_path=str(tmp), s3_flush_records=1)
        store = S3OutputStore(storage, SpiderRuntimeConfig(), "job", "retry", datetime.now(SEOUL), fake)
        item = {"site_id": "fmkorea", "board_id": "humor", "document_id_raw": "123"}
        first = await store.append_post(item)
        assert not first.durable and store.closed
        assert await store.flush_pending() == [("fmkorea", "humor", "123")]
        assert not store.closed

    with TemporaryDirectory() as folder:
        anyio.run(exercise, Path(folder))
