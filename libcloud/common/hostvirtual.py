# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

try:
    import simplejson as json
except ImportError:
    import json

from libcloud.utils.py3 import httplib
from libcloud.common.base import (
    ConnectionKey, JsonResponse)
from libcloud.compute.types import InvalidCredsError
from libcloud.compute.base import (
    NodeAuthSSHKey, NodeAuthPassword, Node)
from libcloud.common.types import LibcloudError
from libcloud.compute.ssh import ParamikoSSHClient
import paramiko


class NAParamikoSSHClient(ParamikoSSHClient):

    def __init__(self, hostname, port=22, username='root', password=None,
                 key=None, key_files=None, key_material=None, timeout=None,
                 allow_agent=False):
        """
        Authentication is always attempted in the following order:

        - The key passed in (if key is provided)
        - Any key we can find through an SSH agent (only if no password and
          key is provided)
        - Any "id_rsa" or "id_dsa" key discoverable in ~/.ssh/ (only if no
          password and key is provided)
        - Plain username/password auth, if a password was given (if password is
          provided)
        """
        if key_files and key_material:
            raise ValueError(('key_files and key_material arguments are '
                              'mutually exclusive'))

        super(NAParamikoSSHClient, self).__init__(hostname=hostname, port=port,
                                                  username=username,
                                                  password=password,
                                                  key=key,
                                                  key_files=key_files,
                                                  timeout=timeout)
        self.allow_agent = allow_agent

    def connect(self):
        conninfo = {'hostname': self.hostname,
                    'port': self.port,
                    'username': self.username,
                    'allow_agent': self.allow_agent,
                    'look_for_keys': self.allow_agent}

        if self.password:
            conninfo['password'] = self.password

        if self.key_files:
            conninfo['key_filename'] = self.key_files

        if self.key_material:
            conninfo['pkey'] = self._get_pkey_object(key=self.key_material)

        if not self.password and not (self.key_files or self.key_material):
            conninfo['allow_agent'] = True
            conninfo['look_for_keys'] = True

        if self.timeout:
            conninfo['timeout'] = self.timeout

        extra = {'_hostname': self.hostname, '_port': self.port,
                 '_username': self.username, '_timeout': self.timeout}
        self.logger.debug('Connecting to server', extra=extra)

        self.client.connect(**conninfo)
        return True


# Version 1
API_VARS = {
    "v1": {
        "API_HOST": "vapi.vr.org",
        "API_ROOT": "",
    },
    "v2": {
        "API_HOST": "oldthing.test",
        "API_ROOT": "/api/legacy",
    },
}


class HostVirtualException(LibcloudError):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        self.args = (code, message)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<HostVirtualException in {0}: {1}>".format(
            self.code, self.message)


class HostVirtualConnection(ConnectionKey):
    host = API_VARS['v1']['API_HOST']

    allow_insecure = False

    def add_default_params(self, params):
        params["key"] = self.key
        return params


class HostVirtualResponse(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.ACCEPTED,
        httplib.CREATED,
        httplib.NO_CONTENT,
    ]

    def parse_body(self):
        if not self.body:
            return None

        data = json.loads(self.body)
        return data

    def parse_error(self):
        data = self.parse_body()

        if self.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError(
                "%(code)s:%(message)s" % (data["error"]))
        elif self.status == httplib.PRECONDITION_FAILED:
            raise HostVirtualException(
                data["error"]["code"], data["error"]["message"])
        elif self.status == httplib.NOT_FOUND:
            raise HostVirtualException(
                data["error"]["code"], data["error"]["message"])

        return self.body

    def success(self):
        return self.status in self.valid_response_codes


class HostVirtualComputeResponse(HostVirtualResponse):
    pass


class HostVirtualComputeConnection(HostVirtualConnection):
    responseCls = HostVirtualComputeResponse


# Version 2
class NetActuateConnection(ConnectionKey):
    host = API_VARS['v2']['API_HOST']

    allow_insecure = True

    def add_default_params(self, params):
        params["key"] = self.key
        return params


class NetActuateNode(Node):
    """This subclass adds a feature usable if integrating
    with pytest-testinfra module. All it does is provide
    a host spec for sshing to Nodes.
    Not required to use.
    """

    def __init__(self, *args, **kwargs):
        self._auth = kwargs.pop("auth", None)
        self._ssh_user = kwargs.pop("ssh_user", "root")
        self._ssh_port = kwargs.pop("ssh_port", "22")
        self.ssh_agent = kwargs.pop("ssh_agent", False)
        if self._auth is not None:
            self._set_auth_method(self._auth)
        else:
            self._set_auth_method(None)
        self._ssh_client = None
        Node.__init__(self, *args, **kwargs)

    @property
    def ssh(self):
        # just return the _ssh client if e have it already
        if self._ssh_client is not None:
            return self._ssh_client

        # just return if we don't have an auth_key
        if self.auth_method is None:
            return

        # no connection, start setting one up using the
        # first private IP
        try:
            hostname = self.public_ips[0]
        except Exception:
            return False

        client_args = {
            "username": self._ssh_user,
            "port": self._ssh_port,
            "allow_agent": self.ssh_agent
        }

        if self.auth_method == "pubkey":

            client_args.update({'allow_agent': True})
            self._ssh_client = NAParamikoSSHClient(hostname, **client_args)

        elif self.auth_method == "password":

            client_args.update(
                {'password': getattr(self._auth, "password")})
            self._ssh_client = NAParamikoSSHClient(hostname, **client_args)

        if self._ssh_client.client.get_transport() is None:
            self._ssh_client.connect()
        return self._ssh_client

    @property
    def auth(self):
        return self._auth

    @auth.setter
    def auth(self, auth_type):
        """Only allow user to set auth as specific type"""
        if (isinstance(auth_type, NodeAuthPassword)) or (
            isinstance(auth_type, NodeAuthSSHKey)
        ):
            self._auth = auth_type
            self._set_auth_method(auth_type)
            if self._ssh_client:
                self._ssh_client.close()
                self._ssh_client = None
        else:
            raise paramiko.ssh_exception.BadAuthenticationType(
                "Only NodeAuthPassword and NodeAuthSSHKey are allowed"
            )

    def _set_auth_method(self, auth):
        if isinstance(auth, NodeAuthPassword):
            self.auth_method = "password"
        elif isinstance(auth, NodeAuthSSHKey):
            self.auth_method = "pubkey"
        elif auth is None:
            self.auth_method = None
        else:
            raise paramiko.ssh_exception.BadAuthenticationType(
                "Only NodeAuthPassword and NodeAuthSSHKey are allowed"
            )


class NetActuateResponse(JsonResponse):
    valid_response_codes = [
        httplib.OK,
        httplib.ACCEPTED,
        httplib.CREATED,
        httplib.NO_CONTENT,
    ]

    def parse_body(self):

        if not self.body:
            return None

        data = json.loads(self.body)
        return data

    def parse_error(self):
        data = self.parse_body()

        if self.status == httplib.UNAUTHORIZED:
            raise InvalidCredsError(
                "%(code)s:%(message)s" % (data["error"]))
        elif self.status == httplib.PRECONDITION_FAILED:
            raise HostVirtualException(
                data["error"]["code"], data["error"]["message"])
        elif self.status == httplib.NOT_FOUND:
            raise HostVirtualException(
                data["error"]["code"], data["error"]["message"])

        return self.body

    def success(self):
        return self.status in self.valid_response_codes


class NetActuateFromDict(object):
    """Takes any dict and creates an object out of it
    May behave weirdly if you do multiple level dicts
    So don't...
    """

    def __init__(self, kwargs):
        self.__dict__ = kwargs

    def __len__(self):
        return len(self.__dict__)


class NetActuateJobStatus(object):
    API_ROOT = API_VARS['v2']['API_ROOT']

    def __init__(
            self,
            conn=None,
            node=None,
            job_result={}):
        self.conn = conn
        self.node = node
        self.job_result = NetActuateFromDict(job_result)
        self._job = self._get_job_status()

    @property
    def status(self):
        return int(self._job.status)

    @property
    def job_id(self):
        return getattr(self._job, "id", 0)

    @property
    def command(self):
        return getattr(self._job, "command", 0)

    @property
    def inserted(self):
        return getattr(self._job, "ts_insert", 0)

    @property
    def started(self):
        return getattr(self._job, "ts_start", 0)

    @property
    def finished(self):
        return getattr(self._job, "ts_finish", 0)

    @property
    def is_success(self):
        return self.status == 5

    @property
    def is_processing(self):
        return self.status == 3

    @property
    def is_failure(self):
        return self.status == 6

    def _get_job_status(self):
        result = self.conn.request(
            "{0}/cloud/jobstatus/{1}/{2}"
            .format(self.API_ROOT,
                    self.job_result.queue_name,
                    self.job_result.id)).object
        return NetActuateFromDict(result) if result else {}

    def refresh(self):
        self._job = self._get_job_status()
        return self


class NetActuateComputeResponse(NetActuateResponse):
    pass


class NetActuateComputeConnection(NetActuateConnection):
    responseCls = NetActuateComputeResponse


def dummyLogger(*args, **kwargs):
    pass
