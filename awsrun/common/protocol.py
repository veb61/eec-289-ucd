import json
from abc import ABC, abstractmethod
import objectfactory
from common.configuration import CmdConfig, WSConfig
from common.resources import File, Folder
from utils.Meta import reconcile_meta


class IMessage(ABC):
    @abstractmethod
    def flatten(self):
        pass


class AWSMsg(reconcile_meta(objectfactory.Serializable, IMessage, ABC)):
    def flatten(self):
        return json.dumps(self.serialize())


@objectfactory.Factory.register_class
class TestConfirmation(AWSMsg):
    _email = objectfactory.Field()

    def __init__(self, email):
        self._email = email

    @property
    def email(self):
        return self._email


@objectfactory.Factory.register_class
class AWSIDRegistration(AWSMsg):
    _awsid = objectfactory.Field()
    _email = objectfactory.Field()

    def __init__(self, id, email):
        self._awsid = id
        self._email = email

    @property
    def id(self):
        return self._awsid

    @property
    def email(self):
        return self._email


@objectfactory.Factory.register_class
class IOTask(AWSMsg):
    _cmdconfig = objectfactory.Nested()
    _wsconfig = objectfactory.Nested()
    _localwd = objectfactory.Field()
    _pfile = objectfactory.Field()

    def __init__(self, cmdconfig: CmdConfig, wsconfig: WSConfig, localwd, perf_file):
        self._cmdconfig = cmdconfig
        self._wsconfig = wsconfig
        self._localwd = localwd
        self._perf_file = perf_file

    @property
    def command(self):
        return self._cmdconfig

    @property
    def workspace(self):
        return self._wsconfig

    @property
    def lwd(self):
        return self._localwd

    @property
    def perf_file(self):
        return self._perf_file

    @property
    def cores(self):
        return self.command.cores
