"""Sync tests."""
from collections import namedtuple

import pytest
from mock import patch
import os
import random
import string
import boto3

ROW = namedtuple("Row", ["data", "xid"])


@pytest.mark.usefixtures("table_creator")
class TestSync(object):
    """Sync tests."""

    def test_logical_slot_changes(self, sync):
        with patch("pgsync.sync.Sync.logical_slot_peek_changes") as mock_peek:
            mock_peek.return_value = [
                ROW("BEGIN: blah", 1234),
            ]
            with patch("pgsync.sync.Sync.sync_payloads") as mock_sync_payloads:
                sync.logical_slot_changes()
                mock_peek.assert_called_once_with(
                    "testdb_testdb",
                    txmin=None,
                    txmax=None,
                    upto_nchanges=None,
                )
                mock_sync_payloads.assert_not_called()

        with patch("pgsync.sync.Sync.logical_slot_peek_changes") as mock_peek:
            mock_peek.return_value = [
                ROW("COMMIT: blah", 1234),
            ]
            with patch("pgsync.sync.Sync.sync_payloads") as mock_sync_payloads:
                sync.logical_slot_changes()
                mock_peek.assert_called_once_with(
                    "testdb_testdb",
                    txmin=None,
                    txmax=None,
                    upto_nchanges=None,
                )
                mock_sync_payloads.assert_not_called()

        with patch("pgsync.sync.Sync.logical_slot_peek_changes") as mock_peek:
            mock_peek.return_value = [
                ROW(
                    "table public.book: INSERT: id[integer]:10 isbn[character "
                    "varying]:'888' title[character varying]:'My book title' "
                    "description[character varying]:null copyright[character "
                    "varying]:null tags[jsonb]:null publisher_id[integer]:null",
                    1234,
                ),
            ]
            with patch(
                "pgsync.sync.Sync.logical_slot_get_changes"
            ) as mock_get:
                with patch(
                    "pgsync.sync.Sync.sync_payloads"
                ) as mock_sync_payloads:
                    sync.logical_slot_changes()
                    mock_peek.assert_called_once_with(
                        "testdb_testdb",
                        txmin=None,
                        txmax=None,
                        upto_nchanges=None,
                    )
                    mock_get.assert_called_once()
                    mock_sync_payloads.assert_called_once()

    @pytest.mark.skip(reason="let's not bang on s3 when unit testing")
    def test_checkpoint_file_in_s3(self, sync):
        os.environ['CHECKPOINT_FILE_IN_S3'] = "True"
        random_string = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
        random_bucket = f"finitestate-firmware-env-pgsync-{random_string}"
        os.environ['CHECKPOINT_FILE_S3_BUCKET'] = random_bucket
        random_int = random.randint(0,1234)
        sync.checkpoint = random_int
        assert sync.checkpoint == random_int
        assert sync.checkpoint_from_s3
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(random_bucket)
        bucket.objects.all().delete()
        bucket.delete()
    
    @pytest.mark.skip(reason="let's not bang on s3 when unit testing")
    def test_checkpoint_file_in_s3_set_twice(self, sync):
        os.environ['CHECKPOINT_FILE_IN_S3'] = "True"
        random_string = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
        random_bucket = f"finitestate-firmware-env-pgsync-{random_string}"
        os.environ['CHECKPOINT_FILE_S3_BUCKET'] = random_bucket
        random_int = random.randint(0,1234)
        sync.checkpoint = random_int
        assert sync.checkpoint == random_int
        assert sync.checkpoint_from_s3
        random_int += 1
        sync.checkpoint = random_int
        assert sync.checkpoint == random_int
        assert sync.checkpoint_from_s3
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(random_bucket)
        bucket.objects.all().delete()
        bucket.delete()

    @pytest.mark.skip(reason="let's not bang on s3 when unit testing")
    def test_checkpoint_file_not_in_s3(self, sync):
        os.environ['CHECKPOINT_FILE_IN_S3'] = "False"
        random_int = random.randint(0,1234)
        sync.checkpoint = random_int
        assert sync.checkpoint == random_int
        assert not sync.checkpoint_from_s3