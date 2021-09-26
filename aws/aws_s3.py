import os
import json
import logging
import sys
import threading
import boto3
import enum
from boto3_type_annotations.s3 import ServiceResource, Bucket
from botocore.exceptions import ClientError

default_region = 'us-west-1'


# Enum for size units
class SIZE_UNIT(enum.Enum):
    BYTES = 1
    KB = 2
    MB = 3
    GB = 4


def convert_unit(size_in_bytes, unit):
    """ Convert the size from bytes to other units like KB, MB or GB"""
    if unit == SIZE_UNIT.KB:
        return size_in_bytes / 1024
    elif unit == SIZE_UNIT.MB:
        return size_in_bytes / (1024 * 1024)
    elif unit == SIZE_UNIT.GB:
        return size_in_bytes / (1024 * 1024 * 1024)
    else:
        return size_in_bytes


def get_file_size(file_name, size_type=SIZE_UNIT.MB):
    """ Get file in size in given unit like KB, MB or GB"""
    size = os.path.getsize(file_name)
    return convert_unit(size, size_type)


class TransferProgress:
    def __init__(self, target_size):
        self._target_size = target_size
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}

    def __call__(self, bytes_transferred):
        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size * 1024 * 1024
            sys.stdout.write(
                f"\r{self._total_transferred} of {target} transferred "
                f"({(self._total_transferred / target) * 100:.2f}%).")
            sys.stdout.flush()


class S3Handler:
    def __init__(self, location):
        self.s3: ServiceResource = boto3.resource('s3')
        self.location = location
        self.logger = logging.getLogger(S3Handler.__class__.__name__)

    def create_bucket(self, name):
        try:
            bucket = self.s3.create_bucket(Bucket=name,
                                           CreateBucketConfiguration={
                                               'LocationConstraint': self.location
                                           }
                                           )
            bucket.wait_until_exists()
            self.logger.info("Created bucket '%s' in region=%s", bucket.name,
                             self.s3.meta.client.meta.region_name)
        except ClientError as error:
            self.logger.exception("Couldn't create bucket named '%s'.",
                                  name)
            raise error
        else:
            return bucket

    def bucket_exists(self, bucket_name):
        try:
            self.s3.meta.client.head_bucket(Bucket=bucket_name)
            exists = True
        except ClientError:
            exists = False
        return exists

    def get_buckets(self):
        try:
            buckets = list(self.s3.buckets.all())
            self.logger.info("Got buckets: %s.", buckets)
        except ClientError:
            self.logger.exception("Couldn't get buckets.")
            raise
        else:
            return buckets

    def get_bucket(self, bucket_name):
        return self.s3.Bucket(bucket_name)

    def get_or_create_bucket(self, bucket_name):
        if self.bucket_exists(bucket_name):
            return self.get_bucket(bucket_name)
        else:
            return self.create_bucket(bucket_name)

    def delete_bucket(self, bucket: Bucket):
        try:
            bucket.delete()
            bucket.wait_until_not_exists()
            self.logger.info("Bucket %s successfully deleted.", bucket.name)
        except ClientError:
            self.logger.exception("Couldn't delete bucket %s.", bucket.name)
            raise

    def get_acl(self, bucket_name):
        try:
            acl = self.s3.Bucket(bucket_name).Acl()
            self.logger.info("Got ACL for bucket %s owned by %s.",
                             bucket_name, acl.owner['DisplayName'])
        except ClientError:
            self.logger.exception("Couldn't get ACL for bucket %s.", bucket_name)
            raise
        else:
            return acl

    def put_policy(self, bucket_name, policy):
        try:
            # The policy must be in JSON format.
            self.s3.Bucket(bucket_name).Policy().put(Policy=json.dumps(policy))
            self.logger.info("Put policy %s for bucket '%s'.", policy, bucket_name)
        except ClientError:
            self.logger.exception("Couldn't apply policy to bucket '%s'.", bucket_name)
            raise

    def get_policy(self, bucket_name):
        try:
            policy = self.s3.Bucket(bucket_name).Policy()
            self.logger.info("Got policy %s for bucket '%s'.", policy.policy, bucket_name)
        except ClientError:
            self.logger.exception("Couldn't get policy for bucket '%s'.", bucket_name)
            raise
        else:
            return json.loads(policy.policy)

    def delete_policy(self, bucket_name):
        try:
            self.s3.Bucket(bucket_name).Policy().delete()
            self.logger.info("Deleted policy for bucket '%s'.", bucket_name)
        except ClientError:
            self.logger.exception("Couldn't delete policy for bucket '%s'.", bucket_name)
            raise

    def upload_public(self, local_file_path, bucket_name, object_key,
                      file_size_mb):
        s3 = self.s3
        extra_args = {
            "ACL": "public-read"
        }

        tcb = TransferProgress(file_size_mb)
        s3.Bucket(bucket_name).upload_file(
            local_file_path,
            object_key,
            ExtraArgs=extra_args,
            Callback=tcb)
        return tcb.thread_info

    def upload_bucket_private(self, local_file_path, bucket_name, object_key, file_size_mb):
        s3 = self.s3
        extra_args = {
            "ACL": "bucket-owner-full-control"
        }
        tcb = TransferProgress(file_size_mb)
        s3.Bucket(bucket_name).upload_file(
            local_file_path,
            object_key,
            ExtraArgs=extra_args,
            Callback=tcb
        )

        return tcb.thread_info

    def upload_file(self, local_file_path, bucket_name, object_key,
                    file_size_mb, sse_key=None, metadata=None):
        s3 = self.s3
        extra_args = {}
        if sse_key:
            extra_args['SSECustomerAlgorithm'] = 'AES256'
            extra_args['SSECustomerKey'] = sse_key

        if metadata:
            extra_args['Metadata'] = metadata

        if not extra_args:
            extra_args = None

        tcb = TransferProgress(file_size_mb)
        s3.Bucket(bucket_name).upload_file(
            local_file_path,
            object_key,
            ExtraArgs=extra_args,
            Callback=tcb)

        return tcb.thread_info

    def download_file(self, bucket_name, object_key, target_path,
                      file_size_mb=None, sse_key=None):
        s3 = self.s3
        if file_size_mb:
            tcb = TransferProgress(file_size_mb)
        else:
            tcb = None

        if sse_key:
            extra_args = {
                'SSECustomerAlgorithm': 'AES256',
                'SSECustomerKey': sse_key}
        else:
            extra_args = None

        s3.Bucket(bucket_name).Object(object_key).download_file(
            target_path,
            ExtraArgs=extra_args,
            Callback=tcb)

        if tcb:
            return tcb.thread_info
