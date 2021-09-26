import os

import objectfactory
from botocore.exceptions import ClientError
from aws import S3Handler, get_file_size, SqsHandler
from abc import ABC, abstractmethod
import tarfile
import time
import datetime

from common.protocol import AWSMsg
from common import resources
from common.resources import Path, File, Folder, S3Path, OSPath
from utils.Meta import reconcile_meta


class Command(ABC):
    @abstractmethod
    def execute(self):
        pass


class Compress(Command):
    @staticmethod
    def is_tar(filename):
        return filename.endswith(".tar")

    def __init__(self, tarfile: File, *required: OSPath):
        if self.is_tar(tarfile.path):
            self._tarfile = tarfile
            self._required = required
        else:
            raise RuntimeError("Not a tarfile!!!")

    def execute(self):
        with tarfile.open(self._tarfile.path, "w") as tarball:
            tarball.dereference = True
            for path in map(lambda c: c.path, self._required):
                try:
                    tarball.add(path)
                except FileNotFoundError:
                    pass  # ignore since all cli args are treated as file paths
        return self._tarfile


class Decompress(Command):
    def __init__(self, target: Folder, tarfile: File, *filter: OSPath):
        if Compress.is_tar(tarfile.path) and os.path.exists(tarfile.path):
            self._tarfile = tarfile
            self._target = target
            self._filter = filter
        else:
            raise RuntimeError("Not a tar file!!!")

    def execute(self):
        self._target.create()
        filteredmembers = tuple(map(lambda c: c.path, self._filter))
        with tarfile.open(self._tarfile.path, "r") as tarball:
            if not filteredmembers:
                tarball.extractall(self._target.path)
            else:
                for member in filteredmembers:
                    tarball.extract(member, self._target.path)
        return self._target


class AWSCommand(Command, ABC):
    def __init__(self, servepath: Path):
        self._serverpath = servepath


class BucketCommand(AWSCommand, ABC):
    def __init__(self, serverpath: Path, bucketpath: Path, file: S3Path):
        super().__init__(serverpath)
        self._bucketpath = bucketpath
        self._s3file = file


class Upload(BucketCommand):
    def __init__(self, serverpath: Path, bucketpath: Path, file: S3Path):
        super().__init__(serverpath, bucketpath, file)

    def execute(self):
        s3handler = S3Handler(location=self._serverpath.path)
        s3handler.upload_bucket_private(self._s3file.path,
                                        self._bucketpath.path,
                                        self._s3file.key,
                                        get_file_size(self._s3file.path))
        return self._s3file


class Download(BucketCommand):
    def __init__(self, serverpath: Path, bucketpath: Path, file: S3Path, timeout):
        super().__init__(serverpath, bucketpath, file)
        self._timeout = timeout

    def execute(self):
        s3handler = S3Handler(location=self._serverpath.path)
        deadline = datetime.datetime.now() + datetime.timedelta(seconds=self._timeout)
        completed = False
        while deadline > datetime.datetime.now():
            try:
                s3handler.download_file(self._bucketpath.path, self._s3file.key, self._s3file.path)
            except ClientError as e:
                error_code = int(e.response["Error"]["Code"])
                if error_code != 404 and error_code != 403:
                    raise e
                time.sleep(1)
            else:
                completed = True
                break
        return self._s3file


class QueueCommand(AWSCommand, ABC):
    def __init__(self, serverpath: Path, queuepath: resources.URL):
        super().__init__(serverpath)
        self._qpath = queuepath


class SendMsg(QueueCommand):
    def execute(self):
        sqs = SqsHandler(self._serverpath.path)
        queue = sqs.get_queue_by_url(self._qpath.path)
        return sqs.send_message(queue,self._msg.flatten())

    def __init__(self, serverpath: Path, queuepath: resources.URL, msg: AWSMsg):
        super().__init__(serverpath, queuepath)
        self._msg = msg
