"""Checkpoint S3 tests."""
from collections import namedtuple

import pytest
from mock import patch
import os
from os import path
import random
import string
import boto3
from botocore.exceptions import ClientError

@pytest.mark.usefixtures("table_creator")
class TestCheckpointS3(object):
    """Checkpoint S3 tests."""

    @pytest.fixture
    def s3_bucket(self):
        os.environ['CHECKPOINT_FILE_IN_S3'] = "True"
        random_string = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
        random_bucket  = f"finitestate-firmware-env-pgsync-{random_string}"
        yield random_bucket
        os.environ['CHECKPOINT_FILE_IN_S3'] = "False"
        s3_resource = boto3.resource('s3')
        bucket = s3_resource.Bucket(random_bucket)
        if s3_resource.Bucket(random_bucket).creation_date is not None:
            bucket.objects.all().delete()
            bucket.delete()

    @pytest.mark.skip(reason="let's not bang on s3 when unit testing")
    def test_checkpoint_file_in_s3(self, sync, s3_bucket):
        os.environ['CHECKPOINT_FILE_S3_BUCKET'] = s3_bucket
        random_int = random.randint(0,1234)
        sync.checkpoint = random_int
        assert sync.checkpoint == random_int
        assert sync.checkpoint_from_s3
        # make sure we're keeping the checkpoint txid around for recovery
        with open(sync._checkpoint_file, "r") as fp:
            assert random_int == int(fp.read().split()[0])
    
    @pytest.mark.skip(reason="let's not bang on s3 when unit testing")
    def test_checkpoint_file_in_s3_set_twice(self, sync, s3_bucket):
        os.environ['CHECKPOINT_FILE_S3_BUCKET'] = s3_bucket
        random_int = random.randint(0,1234)
        sync.checkpoint = random_int
        assert sync.checkpoint == random_int
        assert sync.checkpoint_from_s3
        random_int += 1
        sync.checkpoint = random_int
        assert sync.checkpoint == random_int
        assert sync.checkpoint_from_s3
        # make sure we're keeping the checkpoint txid around for recovery
        with open(sync._checkpoint_file, "r") as fp:
            assert random_int == int(fp.read().split()[0])
    
    @pytest.mark.skip(reason="let's not bang on s3 when unit testing")
    def test_exception_on_s3_error(self, sync, s3_bucket):
        with pytest.raises(ClientError):
            os.environ['CHECKPOINT_FILE_S3_BUCKET'] = s3_bucket
            random_int = random.randint(0,1234)
            sync.checkpoint = random_int
            os.environ['CHECKPOINT_FILE_S3_BUCKET'] = s3_bucket + "-bogus"
            assert sync.checkpoint == random_int

    def test_checkpoint_file_not_in_s3(self, sync):
        os.environ['CHECKPOINT_FILE_IN_S3'] = "False"
        random_int = random.randint(0,1234)
        sync.checkpoint = random_int
        assert sync.checkpoint == random_int
        assert not sync.checkpoint_from_s3