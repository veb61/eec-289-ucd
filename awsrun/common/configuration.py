import json
import os
import pathlib
import uuid
from datetime import datetime
import objectfactory
from common import resources
from common.resources import Folder, S3Path, OSPath, Path

# KEY VALUES FOR CONFIG
REGION = 'REGION'
FILES = 'BUCKET'
TASKS = 'TQUEUE'
REGISTRY = 'RQUEUE'


class AWSConfig:
    def __init__(self, cfg):
        self._config = cfg

    @staticmethod
    def load_url(urlstr):
        url = resources.URL(urlstr)
        if url.isvalid():
            return AWSConfig(url.read())
        else:
            raise RuntimeError("url is invalid")

    @staticmethod
    def load_file(file):
        if os.path.exists(file):
            with open(file) as cfg:
                return AWSConfig(json.load(cfg))
        else:
            raise RuntimeError("configuration not found!!!")

    @property
    def config(self):
        return self._config

    @property
    def serverpath(self):
        return Path(self._config[REGION])

    @property
    def bucketpath(self):
        return Path(self._config[FILES])

    @property
    def taskpath(self):
        return Path(self._config[TASKS])

    @property
    def regpath(self):
        return Path(self._config[REGISTRY])


@objectfactory.Factory.register_class
class WSConfig(objectfactory.Serializable):
    _wsfolder = objectfactory.Field()
    _targetprefix = objectfactory.Field()

    def __init__(self, tgtprefix):
        self._wsfolder = self.unique_root()
        self._targetprefix = tgtprefix

    @property
    def root(self):
        return Folder(self._wsfolder)

    @property
    def local_input(self):
        return self._wsfolder + "_in.tar"

    @property
    def input(self):
        return S3Path(self.local_input, self._generate_key(self.local_input))

    @property
    def local_output(self):
        return self._wsfolder + "_out.tar"

    @property
    def output(self):
        return S3Path(self.local_output, self._generate_key(self.local_output))

    def _generate_key(self, path):
        return self._targetprefix + os.path.sep + self._wsfolder + os.path.sep + path

    @staticmethod
    def unique_root(prefix="ws"):
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        return prefix + "_" + timestamp + "_" + str(uuid.uuid4())

@objectfactory.Factory.register_class
class CmdConfig(objectfactory.Serializable):
    _command = objectfactory.Field()
    _timeout = objectfactory.Field()
    _cores = objectfactory.Field()
    _depcfg = objectfactory.Field()

    def __init__(self, cmd, timeout, cores, depfile):
        self._command = cmd
        self._timeout = timeout
        self._cores = cores
        self._depcfg = depfile

    @property
    def shell(self):
        return self._command

    @property
    def timeout(self):
        return self._timeout

    @property
    def cores(self):
        return self._cores

    @property
    def deps(self):
        try:
            with open(self._depcfg, "r") as f:
                extra_files = list(f.readlines())
        except FileNotFoundError:
            return []
        else:
            cleaned_extra_files = []
            for extra_file in extra_files:
                stripped_extra_file = extra_file.strip()
                if len(stripped_extra_file) > 0:
                    for x in pathlib.Path(".").glob(stripped_extra_file):
                        cleaned_extra_files.append(OSPath.new(str(x)))
            return cleaned_extra_files
