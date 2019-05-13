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
    ConnectionKey, JsonResponse, ConnectionUserAndKey)
from libcloud.compute.types import InvalidCredsError
from libcloud.compute.base import (
    NodeAuthSSHKey, NodeAuthPassword, Node)
from libcloud.common.types import LibcloudError
from libcloud.compute.ssh import ParamikoSSHClient
import paramiko

API_HOST_v1 = "vapi.vr.org"
API_HOST_v2 = "staging-portal4.netactuate.com/api/legacy"
API_ROOT = ""
# Version 1


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
    host = API_HOST_v1

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
    host = API_HOST_v2

    allow_insecure = False

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
        if self._auth is not None:
            self._set_auth_method(self._auth)
        else:
            self.auth_method = None
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
            hostname = self.private_ips[0]
        except Exception:
            return False

        if self.auth_method == "pubkey":

            self._ssh_client = ParamikoSSHClient(
                hostname, key=getattr(self._auth, "pubkey"),
                username=self._ssh_user
            )
        elif self.auth_method == "password":
            self._ssh_client = ParamikoSSHClient(
                hostname,
                username=self._ssh_user,
                password=getattr(self._auth, "password"),
            )
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

        # data = json.loads(self.body)
        return self.body  # data

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
    def __init__(self, conn=None, node=None, job_result={}):
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
    def is_success(self):
        return self.status == 5

    @property
    def is_processing(self):
        return self.status == 3

    @property
    def is_failure(self):
        return self.status == 6

    def _get_job_status(self):
        params = {"mbpkgid": self.node.id, "job_id": self.job_result.id}
        result = self.conn.request(
            API_ROOT + "/cloud/serverjob", params=params).object
        return NetActuateFromDict(result)

    def refresh(self):
        self._job = self._get_job_status()
        return self


class NetActuateComputeResponse(NetActuateResponse):
    pass


class NetActuateComputeConnection(NetActuateConnection):
    responseCls = NetActuateComputeResponse
