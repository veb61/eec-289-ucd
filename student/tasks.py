import os
import time
from abc import ABC, abstractmethod, ABCMeta

from common.commands import Compress, Upload, SendMsg, Download, Decompress
from common.configuration import AWSConfig
from common.protocol import IOTask, AWSMsg, AWSIDRegistration
from common.resources import Folder, File, OSPath
from multipledispatch import dispatch


class Issuer(ABC):
    @abstractmethod
    def issue(self, task: AWSMsg):
        pass


class AWSIssuer(Issuer):
    def __init__(self, awsconfig: AWSConfig):
        self._awsconfig = awsconfig

    @staticmethod
    def dependencies(task: IOTask):
        deps = []
        cwd = Folder.cwd()
        deps.extend(map(lambda f: cwd.relative(f),
                        map(lambda p: OSPath.new(p), filter(lambda arg: os.path.exists(arg), task.command.shell))))
        deps.extend(map(lambda f: cwd.relative(f), task.command.deps))
        return deps

    def _operands(self, task: IOTask):
        resources = Compress(task.workspace.input, *AWSIssuer.dependencies(task)).execute()
        uploaded = Upload(self._awsconfig.serverpath, self._awsconfig.bucketpath, resources).execute()
        # Echo status back to user.
        print("Resources {0} is transfered\n".format(uploaded.path))
        time.sleep(1)

    def _operator(self, task: IOTask):
        return SendMsg(self._awsconfig.serverpath, self._awsconfig.taskpath, task).execute()

    def _clean_files(self, task: IOTask):
        os.remove(task.workspace.local_input)
        os.remove(task.workspace.local_output)

    def _output(self, task: IOTask):
        retrieved = Download(self._awsconfig.serverpath, self._awsconfig.bucketpath, task.workspace.output,
                             task.command.timeout).execute()
        cwd = Folder(os.path.normpath(os.getcwd()))
        # files to extract
        stdout_report = File('stdout')
        stderr_report = File('stderr')
        target = Decompress(cwd, retrieved, stdout_report, stderr_report).execute()
        # report
        target.relative(stdout_report).content(header=" STDOUT ")
        target.relative(stderr_report).content(header=" STDERR ")
        #
        if task.perf_file:
            Decompress(task.lwd.relative(task.workspace.root).create(), task.workspace.local_input).execute()

        self._clean_files(task)

    @dispatch(IOTask)
    def issue(self, task):
        self._operands(task)
        self._operator(task)
        self._output(task)

    @dispatch(AWSIDRegistration)
    def issue(self, reg):
        SendMsg(self._awsconfig.serverpath, self._awsconfig.regpath, reg).execute()
