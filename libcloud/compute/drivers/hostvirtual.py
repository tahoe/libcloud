# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
libcloud driver for the Host Virtual Inc. (VR) API
Home page https://www.hostvirtual.com/
"""

import time
import re

try:
    import simplejson as json
except ImportError:
    import json
import requests
from libcloud.common.hostvirtual import (
    NetActuateJobStatus, NetActuateNode, HostVirtualException,
    NetActuateComputeConnection, HostVirtualComputeConnection,
    API_VARS, dummyLogger
)
from libcloud.compute.providers import Provider
from libcloud.compute.types import NodeState
from libcloud.compute.base import Node, NodeDriver
from libcloud.compute.base import NodeImage, NodeSize, NodeLocation
from libcloud.compute.base import NodeAuthSSHKey, NodeAuthPassword

DEFAULT_API_VERSION = 'v2'

HV_NODE_STATE_MAP = {
    'BUILDING': NodeState.PENDING,
    'PENDING': NodeState.PENDING,
    'RUNNING': NodeState.RUNNING,  # server is powered up
    'STOPPING': NodeState.REBOOTING,
    'REBOOTING': NodeState.REBOOTING,
    'STARTING': NodeState.REBOOTING,
    'TERMINATED': NodeState.TERMINATED,  # server is powered down
    'STOPPED': NodeState.STOPPED,
}

NA_NODE_STATE_MAP = {
    'BUILDING': NodeState.PENDING,
    'PENDING': NodeState.PENDING,
    'RUNNING': NodeState.RUNNING,  # server is powered up
    'STOPPING': NodeState.REBOOTING,
    'REBOOTING': NodeState.REBOOTING,
    'STARTING': NodeState.REBOOTING,
    'TERMINATED': NodeState.TERMINATED,  # server is powered down
    'TERMINATING': NodeState.TERMINATED,
    'STOPPED': NodeState.STOPPED,
    'ERROR': NodeState.UNKNOWN

}

DEFAULT_NODE_LOCATION_ID = 21

# hostname regex
NAME_RE = '({0}|{0}{1}*{0})'.format('[a-zA-Z0-9]', r'[a-zA-Z0-9\-]')
HOSTNAME_RE = r'({0}\.)*{0}$'.format(NAME_RE)


def auth_from_path(key_file):
    """Example key_file
    /home/user/.ssh/id_rsa.pub
    """
    with open(key_file) as f:
        key = f.read()
        auth = NodeAuthSSHKey(pubkey=key)
    return auth


class HostVirtualNodeDriver(NodeDriver):
    name = 'HostVirtual'
    website = 'http://www.hostvirtual.com'

    def __new__(cls, key, secret=None, secure=True, host=None, port=None,
                api_version=DEFAULT_API_VERSION, **kwargs):
        if cls is HostVirtualNodeDriver:
            if api_version == '1.0':
                cls = HostVirtualNodeDriver_v1
            elif api_version == '2.0':
                cls = HostVirtualNodeDriver_v2
            else:
                raise NotImplementedError(
                    'Unsupported API version: {0}'
                    .format(api_version))
        return super(HostVirtualNodeDriver, cls).__new__(cls)


class HostVirtualNodeDriver_v1(HostVirtualNodeDriver):
    type = Provider.HOSTVIRTUAL
    name = 'HostVirtual (API V1.0)'
    connectionCls = HostVirtualComputeConnection
    features = {'create_node': ['ssh_key', 'password']}
    API_ROOT = API_VARS['v1']['API_ROOT']

    def __init__(self, key, secure=True, host=None,
                 port=None, api_version='v1'):
        self.location = None
        super(HostVirtualNodeDriver, self).__init__(
            key=key,
            secure=secure,
            host=host, port=port
        )

    def list_nodes(self):
        try:
            result = self.connection.request(
                '{0}/cloud/servers/'
                .format(self.API_ROOT)
            ).object
        except HostVirtualException:
            return []
        nodes = []
        for value in result:
            node = self._to_node(value)
            nodes.append(node)
        return nodes

    def list_sizes(self, location=None):
        if location is None:
            location = ''
        result = self.connection.request(
            '{0}/cloud/sizes/{1}'
            .format(self.API_ROOT, location)
        ).object
        sizes = []
        for size in result:
            n = NodeSize(id=size['plan_id'],
                         name=size['plan'],
                         ram=size['ram'],
                         disk=size['disk'],
                         bandwidth=size['transfer'],
                         price=size['price'],
                         driver=self.connection.driver)
            sizes.append(n)
        return sizes

    def list_locations(self):
        result = self.connection.request(
            '{0}/cloud/locations/'
            .format(self.API_ROOT)
        ).object
        locations = []
        for dc in result.values():
            # set up country, avoiding errors, rather just exclude
            dc_id = dc['id']
            dc_name = dc['name']
            name_split = dc_name.split(',')

            # require that the format of the dc name has a comma
            # with the country name after the comma, otherwise drop it
            if len(name_split) > 1:
                dc_country = name_split[1].replace(" ", "")
            else:
                # drop this location since someone f'd up the name at HV
                continue

            # add the NodeLocation
            locations.append(NodeLocation(
                dc_id,
                dc_name,
                dc_country,
                self))
        return sorted(locations, key=lambda x: int(x.id))

    def create_node(self, name, image, size, **kwargs):
        """
        Creates a node

        Example of node creation with ssh key deployed:

        >>> from libcloud.compute.base import NodeAuthSSHKey
        >>> key = open('/home/user/.ssh/id_rsa.pub').read()
        >>> auth = NodeAuthSSHKey(pubkey=key)
        >>> from libcloud.compute.providers import get_driver
        >>> driver = get_driver('hostvirtual')
        >>> conn = driver('API_KEY')
        >>> image = conn.list_images()[1]
        >>> size = conn.list_sizes()[0]
        >>> location = conn.list_locations()[1]
        >>> name = 'markos-dev'
        >>> node = conn.create_node(name, image, size, auth=auth,
        >>>                         location=location)
        """

        dc = None

        auth = self._get_and_check_auth(kwargs.get('auth'))

        if not self._is_valid_fqdn(name):
            raise HostVirtualException(
                500, "Name should be a valid FQDN (e.g, hostname.example.com)")

        # simply order a package first
        pkg = self.ex_order_package(size)

        if 'location' in kwargs:
            dc = kwargs['location'].id
        else:
            dc = DEFAULT_NODE_LOCATION_ID

        # create a stub node
        stub_node = self._to_node({
            'mbpkgid': pkg['id'],
            'status': 'PENDING',
            'fqdn': name,
            'plan_id': size.id,
            'os_id': image.id,
            'location_id': dc
        })

        # provisioning a server using the stub node
        self.ex_provision_node(node=stub_node, auth=auth)
        node = self._wait_for_node(stub_node.id)
        if getattr(auth, 'generated', False):
            node.extra['password'] = auth.password

        return node

    def reboot_node(self, node):
        mbpkgid = node.id
        result = self.connection.request(
            '{0}/cloud/server/reboot/{1}'
            .format(self.API_ROOT, mbpkgid),
            method='POST').object

        return bool(result)

    def destroy_node(self, node):
        mbpkgid = node.id
        result = self.connection.request(
            '{0}/cloud/cancel/{1}'
            .format(self.API_ROOT, mbpkgid),
            method='POST').object

        return bool(result)

    def list_images(self):
        result = self.connection.request(
            '{0}/cloud/images/'.format(self.API_ROOT)
        ).object
        images = []
        for image in result:
            i = NodeImage(id=image["id"],
                          name=image["os"],
                          driver=self.connection.driver,
                          extra=image)
            del i.extra['id']
            del i.extra['os']
            images.append(i)
        return images

    def ex_list_packages(self):
        """
        List the server packages.

        """

        try:
            result = self.connection.request(
                '{0}/cloud/packages/'.format(self.API_ROOT)
            ).object
        except HostVirtualException:
            return []
        pkgs = []
        for value in result:
            pkgs.append(value)
        return pkgs

    def ex_order_package(self, size):
        """
        Order a server package.

        :param      size:
        :type       node: :class:`NodeSize`

        :rtype: ``str``
        """
        plan = size.name
        return self.connection.request(
            '{0}/cloud/buy/{1}'
            .format(self.API_ROOT, plan),
            method='POST'
        ).object

    def ex_cancel_package(self, node):
        """
        Cancel a server package.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``str``
        """

        result = self.connection.request(
            '{0}/cloud/cancel/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return result

    def ex_unlink_package(self, node):
        """
        Unlink a server package from location.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``str``
        """

        result = self.connection.request(
            '{0}/cloud/unlink/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return result

    def ex_get_node(self, node):
        """
        Get a single node.

        :param      node_id: id of the node that we need the node object for
        :type       node_id: ``str``

        :rtype: :class:`Node`
        """

        result = self.connection.request(
            '{0}/cloud/server/{1}'
            .format(self.API_ROOT, node.id)
        ).object
        node = self._to_node(result)
        return node

    def ex_stop_node(self, node):
        """
        Stop a node.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        result = self.connection.request(
            '{0}/cloud/server/shutdown/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return bool(result)

    def ex_start_node(self, node):
        """
        Start a node.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        result = self.connection.request(
            '{0}/cloud/server/start/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return bool(result)

    def ex_provision_node(self, **kwargs):
        """
        Provision a server on a VR package and get it booted

        :keyword node: node which should be used
        :type    node: :class:`Node`

        :keyword image: The distribution to deploy on your server (mandatory)
        :type    image: :class:`NodeImage`

        :keyword auth: an SSH key or root password (mandatory)
        :type    auth: :class:`NodeAuthSSHKey` or :class:`NodeAuthPassword`

        :keyword location: which datacenter to create the server in
        :type    location: :class:`NodeLocation`

        :return: Node representing the newly built server
        :rtype: :class:`Node`
        """

        node = kwargs['node']

        if 'image' in kwargs:
            image = kwargs['image']
        else:
            image = node.extra['image']

        params = {
            'mbpkgid': node.id,
            'image': image,
            'fqdn': node.name,
            'location': node.extra['location'],
        }

        auth = kwargs['auth']

        ssh_key = None
        password = None
        if isinstance(auth, NodeAuthSSHKey):
            ssh_key = auth.pubkey
            params['ssh_key'] = ssh_key
        elif isinstance(auth, NodeAuthPassword):
            password = auth.password
            params['password'] = password

        if not ssh_key and not password:
            raise HostVirtualException(
                500, "SSH key or Root password is required")

        try:
            result = self.connection.request(
                '{0}/cloud/server/build'
                .format(self.API_ROOT),
                data=json.dumps(params),
                method='POST'
            ).object
            return bool(result)
        except HostVirtualException:
            self.ex_cancel_package(node)

    def ex_delete_node(self, node):
        """
        Delete a node.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """

        result = self.connection.request(
            '{0}/cloud/server/delete/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return bool(result)

    def _to_node(self, data):
        state = HV_NODE_STATE_MAP[data['status']]
        public_ips = []
        private_ips = []
        extra = {}

        if 'plan_id' in data:
            extra['size'] = data['plan_id']
        if 'os_id' in data:
            extra['image'] = data['os_id']
        if 'fqdn' in data:
            extra['fqdn'] = data['fqdn']
        if 'location_id' in data:
            extra['location'] = data['location_id']
        if 'ip' in data:
            public_ips.append(data['ip'])

        node = Node(id=data['mbpkgid'], name=data['fqdn'], state=state,
                    public_ips=public_ips, private_ips=private_ips,
                    driver=self.connection.driver, extra=extra)
        return node

    def _wait_for_node(self, node_id, timeout=30, interval=5.0):
        """
        :param node_id: ID of the node to wait for.
        :type node_id: ``int``

        :param timeout: Timeout (in seconds).
        :type timeout: ``int``

        :param interval: How long to wait (in seconds) between each attempt.
        :type interval: ``float``

        :return: Node representing the newly built server
        :rtype: :class:`Node`
        """
        # poll until we get a node
        for i in range(0, timeout, int(interval)):
            try:
                node = self.ex_get_node(node_id)
                return node
            except HostVirtualException:
                time.sleep(interval)

        raise HostVirtualException(412, 'Timeout on getting node details')

    def _is_valid_fqdn(self, fqdn):
        if len(fqdn) > 255:
            raise HostVirtualException(
                500, "Need a valid FQDN (e.g, hostname.example.com)")
        if fqdn[-1] == ".":
            fqdn = fqdn[:-1]
        if re.match(HOSTNAME_RE, fqdn) is not None:
            return fqdn
        else:
            raise HostVirtualException(
                500, "Need a valid FQDN (e.g, hostname.example.com)")


######
# New driver below based off more recent work for the old driver.
# Only porting features above to fix bugs, nothing else.
######
class HostVirtualNodeDriver_v2(HostVirtualNodeDriver):
    type = Provider.HOSTVIRTUAL
    name = 'HostVirtual (API V2.0)'
    website = 'http://www.netactuate.com'
    connectionCls = NetActuateComputeConnection
    features = {'create_node': ['ssh_key', 'password']}
    API_ROOT = API_VARS['v2']['API_ROOT']

    def __init__(self, key, secure=True, host=None, port=None,
                 logger=dummyLogger, debug=False, auth=None,
                 ssh_key_file=None, ssh_user='root', ssh_port='22',
                 user_password=None, ssh_agent=False, api_version='2.0'):
        self.conn = requests.Session()
        self.debug = debug
        self.local_logger = logger
        self.debug_logger = logger
        self._ssh_user = ssh_user
        self._ssh_key_file = ssh_key_file
        self._ssh_port = ssh_port
        self._ssh_agent = ssh_agent
        self.auth = auth
        if self.auth is None:
            if ssh_key_file is not None:
                self.auth = auth_from_path(ssh_key_file)
            elif user_password is not None:
                self.auth = NodeAuthPassword(user_password)

        super(HostVirtualNodeDriver, self).__init__(
            key=key, secure=secure, host=host, port=port)

    def list_nodes(self):
        try:
            result = self.connection.request(
                '{0}/cloud/servers/'.format(self.API_ROOT)
            ).object
        except HostVirtualException:
            return []
        nodes = []
        for value in result:
            node = self._to_node(value)
            nodes.append(node)
        return nodes

    def list_locations(self):
        result = self.connection.request(
            '{0}/cloud/locations/'.format(self.API_ROOT)
        ).object
        locations = []
        return result
        for dc in result.values():

            # set up country, avoiding errors, rather just exclude
            dc_id = dc['id']
            dc_name = dc['name']
            name_split = dc_name.split(',')

            # require that the format of the dc name has a comma
            # with the country name after the comma, otherwise drop it
            if len(name_split) > 1:
                dc_country = name_split[1].replace(" ", "")
            else:
                # drop this location since someone f'd up the name at HV
                continue

            # add the NodeLocation
            locations.append(NodeLocation(
                dc_id,
                dc_name,
                dc_country,
                self))
        return sorted(locations, key=lambda x: int(x.id))

    def list_sizes(self, location=None):
        if location is None:
            location = ''
        result = self.connection.request(
            '{0}/cloud/sizes/{1}'.format(self.API_ROOT, location)
        ).object

        sizes = []
        for size in result:
            n = NodeSize(id=size['plan_id'],
                         name=size['plan'],
                         ram=size['ram'],
                         disk=size['disk'],
                         bandwidth=size['transfer'],
                         price=0,
                         driver=self.connection.driver)
            sizes.append(n)
        return sizes

    def list_images(self):
        result = self.connection.request(
            '{0}/cloud/images/'.format(self.API_ROOT)
        ).object
        images = []
        for image in result:
            i = NodeImage(id=image["id"],
                          name=image["os"],
                          driver=self.connection.driver,
                          extra=image)
            del i.extra['id']
            del i.extra['os']
            images.append(i)
        return images

    def create_node(self, timeout=600, **kwargs):
        """
        Creates a node

        Example of node creation with ssh key deployed:

        Arguments:  name        str:    fqdn for host
                    image       obj:    NodeImage instance
                    size        obj:    NodeSize instance
                    location    obj:    NodeLocation instance
                    auth        obj:    NodeAuthSSHKey instance
                    mbpkgid     int:    mbpkgid to build, otherwise
                                        we have to order a package
                                        which is not available right now
                    timeout     int:    how long to wait for shit to happen

        Example:
            # import driver and api key
            from hv_libcloud import HostVirtualNodeDriver
            from config import cust_api

            # and that's it, connect, also providing an ssh pub key file
            conn = HostVirtualNodeDriver(
                cust_api,
                ssh_key_file='/home/me/.ssh/id_rsa.pub')
            )

            # get an image object
            image = conn.list_images()[1]
            # get an size object
            size = conn.list_sizes()[0]
            # get an location object
            location = conn.list_locations()[21]
            # set a name
            name = 'qa-test-one.vr.org'
            # set mbpkgid
            mbpkgid = 204581
            # create the node (domU)
            node = conn.create_node(
                name=name, image=image, mbpkgid=mbpkgid,
                size=size, location=location)
        """
        # TODO Do all the parameter checks, except for auth...
        # log where we are
        self.local_logger(level='info',
                          format='We are inside the create_node method')

        dc = None

        # fqdn validity check returns the fqdn
        name = self._is_valid_fqdn(kwargs.get('name'))

        mbpkgid = kwargs.get('mbpkgid', None)
        if mbpkgid is None:
            # simply order a package first
            # NOTE: Commented out, only accepting mbpkgids
            # No buys
            # pkg = self.ex_order_package(kwargs.get('size'))
            raise HostVirtualException(
                500,
                "Must provide an mbpkgid parameter,"
                " we are not doing buys just yet")
        else:
            # creat a package dict from mbpkgid so syntax is correct below
            pkg = {'id': mbpkgid}

        # get the size
        size = kwargs.get('size', None)
        if size is None or not isinstance(size, NodeSize):
            raise HostVirtualException(
                500, "size parameter must be a NodeSize object")

        # get the image
        image = kwargs.get('image', None)
        if image is None or not isinstance(image, NodeImage):
            raise HostVirtualException(
                500, "image parameter must be a NodeImage object")

        # get the location
        location = kwargs.get('location', None)
        if location is not None:
            dc = location.id
        else:
            dc = DEFAULT_NODE_LOCATION_ID

        # create a stub node
        stub_node = self._to_node({
            'mbpkgid': pkg['id'],
            'status': 'PENDING',
            'fqdn': name,
            'plan_id': size.id,
            'os_id': image.id,
            'location_id': dc
        })

        # provisioning a server using the stub node
        provision_job = self.ex_provision_node(node=stub_node)

        # we get a new node object when it's populated
        # this will be updated so is what we want to return
        node = self._wait_for_node(stub_node.id, timeout=timeout)
        if getattr(self.auth, 'generated', False):
            node.extra['password'] = self.auth.password

        # wait for the node to say it's running
        # before we return so callers don't try to do
        # something that requires it to be built
        node = self._wait_for_state(
            node, 'running', job=provision_job, timeout=timeout)
        return node

    def ex_ensure_state(self, node, want_state="running", timeout=600):
        """Main function call that will check desired state
        and call the appropriate function and handle the respones
        back to main.

        The called functions will check node state and call
        state altering functions as needed.

        Returns:    changed bool    whether or not a change of state happend
                    node    obj     instance of Node
        """
        # set default changed to False
        changed = False

        # get the latest updated node only if it exists
        if self.node_exists(node.id):
            node = self.ex_get_node(node.id)

        ###
        # DIE if the node has never been built and we are being
        # asked to uninstall
        # we can't uninstall the OS on a node that isn't even in the DB
        ###
        if not node and want_state == 'terminated':
            raise HostVirtualException(500,
                                       "Cannot uninstall a node that"
                                       " doesn't exist. Please build"
                                       " the package first,"
                                       " then you can uninstall it.")

        # We only need to do any work if the below conditions exist
        # otherwise we will return the defaults
        if node.state != want_state:
            # update state based on the node existing
            if want_state in ('running', 'present'):
                # ensure_running makes sure it is up and running,
                # making sure it is installed also
                changed, node = self._ensure_node_running(
                    node, timeout=timeout)
            if want_state == 'stopped':
                # ensure that the node is stopped, this should include
                # making sure it is installed also
                changed, node = self._ensure_node_stopped(
                    node, timeout=timeout)

            if want_state == 'terminated':
                changed, node = self._ensure_node_terminated(
                    node, timeout=timeout)

            if want_state == 'unlinked':
                changed, node = self._ensure_node_unlinked(
                    node, timeout=timeout)

        # in order to return, we must have a node object
        # and a status (changed) of whether or not
        # state has changed to the desired state
        return changed, node

    def ex_list_packages(self):
        """
        List the server packages.

        """

        try:
            result = self.connection.request(
                '{0}/cloud/packages/'.format(self.API_ROOT)
            )
        except HostVirtualException:
            return []
        return result
        pkgs = []
        for value in result:
            pkgs.append(value)
        return pkgs

    def ex_order_package(self, size):
        """
        Order a server package.

        :param      size:
        :type       node: :class:`NodeSize`

        :rtype: ``str``
        """

        plan = size.name
        pkg = self.connection.request(
            '{0}/cloud/buy/{1}'.format(self.API_ROOT, plan), method='POST'
        ).object

        return pkg

    def ex_cancel_package(self, node):
        """
        Cancel a server package.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``str``
        """

        mbpkgid = node.id
        result = self.connection.request(
            '{0}/cloud/cancel/{1}'.format(mbpkgid), method='POST'
        ).object
        return NetActuateJobStatus(
            conn=self.connection,
            node_id=node.id,
            job_result=result)

    def ex_unlink_package(self, node):
        """
        Unlink a server package from location.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``str``
        """

        mbpkgid = node.id
        result = self.connection.request(
            '{0}/cloud/server/unlink/{1}'.format(self.API_ROOT, mbpkgid),
            method='POST'
        ).object

        return bool(result)

    def node_exists(self, node):
        exists = True
        try:
            self.ex_get_node(node.id)
        except Exception:
            exists = False
        return exists

    def ex_get_node(self, node_id):
        """
        Get a single node.

        :param      node_id: id of the node to retrieve
        :type       node_id: ``str``

        :rtype: :class:`Node`
        """
        result = self.connection.request(
            '{0}/cloud/server/{1}'.format(self.API_ROOT, node_id)).object
        if isinstance(result, list) and len(result) > 0:
            node = self._to_node(result[0])
        elif isinstance(result, dict):
            node = self._to_node(result)
        return node

    def ex_stop_node(self, node):
        """
        Stop a node.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """

        result = self.connection.request(
            '{0}/cloud/server/shutdown/{1}'.format(self.API_ROOT, node.id),
            method='POST').object
        self.debug_logger(format='Here is the stop call return: {0}'
                          .format(result))
        return NetActuateJobStatus(
            conn=self.connection,
            node=node,
            job_result=result)

    def ex_start_node(self, node):
        """
        Start a node.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """
        result = self.connection.request(
            '{0}/cloud/server/start/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object
        self.debug_logger(format='Here is the start call return: {0}'
                          .format(result))

        return NetActuateJobStatus(
            conn=self.connection,
            node=node,
            job_result=result)

    def ex_reboot_node(self, node):
        result = self.connection.request(
            '{0}/cloud/server/reboot/{1}'
            .format(self.API_ROOT, node.id),
            method='POST').object

        return NetActuateJobStatus(
            conn=self.connection,
            node_id=node.id,
            job_result=result)

    def ex_provision_node(self, **kwargs):
        """
        Provision a server on a VR package and get it booted

        :keyword node: node which should be used
        :type    node: :class:`Node`

        :keyword image: The distribution to deploy on your server (mandatory)
        :type    image: :class:`NodeImage`

        :keyword auth: an SSH key or root password (mandatory)
        :type    auth: :class:`NodeAuthSSHKey` or :class:`NodeAuthPassword`

        :keyword location: which datacenter to create the server in
        :type    location: :class:`NodeLocation`

        :return: Node representing the newly built server
        :rtype: :class:`Node`
        """

        node = kwargs['node']

        if 'image' in kwargs:
            image = kwargs['image']
        else:
            image = node.extra['image']

        params = {
            'mbpkgid': node.id,
            'image': image,
            'fqdn': node.name,
            'location': node.extra['location'],
        }

        if self.auth:
            auth = self.auth
        else:
            auth = kwargs.get('auth', None)

        ssh_key = None
        password = None
        if isinstance(auth, NodeAuthSSHKey):
            ssh_key = auth.pubkey
            params['ssh_key'] = ssh_key
        elif isinstance(auth, NodeAuthPassword):
            password = auth.password
            params['password'] = password

        if not ssh_key and not password:
            raise HostVirtualException(
                500, "SSH key or Root password is required")

        result = self.connection.request('{0}/cloud/server/build'
                                         .format(self.API_ROOT),
                                         data=json.dumps(params),
                                         method='POST').object
        self.debug_logger(format='Here is the build call return: {0}'
                          .format(result))
        return NetActuateJobStatus(
            conn=self.connection,
            node=node,
            job_result=result)

    def ex_delete_node(self, node):
        """
        Delete a node.

        :param      node: Node which should be used
        :type       node: :class:`Node`

        :rtype: ``bool``
        """

        mbpkgid = node.id
        result = self.connection.request(
            '{0}/cloud/server/delete/{1}'.format(self.API_ROOT, mbpkgid),
            method='POST').object
        self.debug_logger(format='Here is the delete call return: {0}'
                          .format(result))
        return NetActuateJobStatus(
            conn=self.connection,
            node_id=node.id,
            job_result=result)

    # TODO: Unused
    def _get_location(avail_locs=[], want_location=None):
        """Check if a location is allowed/available

        Raises an exception if we can't use it
        Returns a location object otherwise
        """
        location = None
        loc_possible_list = [loc for loc in avail_locs
                             if (loc.name == want_location or
                                 loc.id == want_location)]

        if not loc_possible_list:
            raise HostVirtualException(
                500, "Sorry, Location {0} doesn't seem to be available"
                .format(want_location))
        else:
            location = loc_possible_list[0]
        return location

    # TODO: Unused
    def _get_os(avail_oses=[], want_os=None):
        """Check if provided os is allowed/available

        Raises an exception if we can't use it
        Returns an image/OS object otherwise
        """
        image = None
        os_possible_list = [opsys for opsys in avail_oses
                            if opsys.name == want_os or opsys.id == want_os]

        if not os_possible_list:
            raise HostVirtualException(
                500, "Sorry, OS {0} doesn't seem to be available"
                .format(want_os))
        else:
            image = os_possible_list[0]
        return image
        ##
        # END Migrated functions not used yet
        ##

    def _to_node(self, data):
        state = NA_NODE_STATE_MAP[data['status']]
        public_ips = []
        private_ips = []
        extra = {}

        if 'plan_id' in data:
            extra['size'] = data['plan_id']
        if 'os_id' in data:
            extra['image'] = data['os_id']
        if 'fqdn' in data:
            extra['fqdn'] = data['fqdn']
        if 'location_id' in data:
            extra['location'] = data['location_id']
        if 'ip' in data and data['ip']:
            public_ips.append(data['ip'])

        node = NetActuateNode(id=data['mbpkgid'], name=data['fqdn'],
                              state=state, public_ips=public_ips,
                              private_ips=private_ips, extra=extra,
                              driver=self.connection.driver,
                              ssh_user=self._ssh_user,
                              ssh_agent=self._ssh_agent,
                              auth=self.auth, ssh_port=self._ssh_port)
        return node

    def _wait_for_unlinked(self, node, timeout=600, interval=10):
        """Special state check for making sure a node is unlinked
        Arguments:
            node:               obj     Node object
            timeout:            int     timeout in seconds
            interval:           float   sleep time between loops

        Return:     Bool        True if unlinked
                    Exception   raised if not unlinked
        """
        for i in range(0, timeout, int(interval)):
            if not self.node_exists(node.id):
                node.state = 'unlinked'
                self.local_logger(
                    level='info',
                    format='We reached a state of {0}!'
                           .format('unlinked'))
                break
            time.sleep(interval)
        return node

    def _wait_for_state(self, node, want_state,
                        job=None, timeout=600, interval=10):
        """Called after do_build_node to wait to make sure it built OK
        Arguments:
            node:               obj     Node object
            timeout:            int     timeout in seconds
            interval:           float   sleep time between loops
            want_state:         string  string of the desired state
        """
        if job is None:
            raise AttributeError("Job object required")
        try_node = None
        for i in range(0, timeout, int(interval)):
            self.local_logger(
                level='info',
                format='Checking to see if state is {0}'.format(want_state))
            try_node = self.ex_get_node(node.id)
            # NOTE: We should be logging both at the same time but we'll see
            # if job.refresh().is_success:
            #    self.local_logger(
            #        level='info',
            #        format='{0} job status is 5 for job id {1}'
            #               .format(job.command, job.job_id))
            if try_node.state == want_state:
                self.local_logger(
                    level='info',
                    format='We reached a state of {0}!'
                           .format(want_state))
                break
            time.sleep(interval)
        # this is to avoid a bug in the backend, won't always work though
        time.sleep(15)
        return try_node

    def _wait_for_node(self, node_id, timeout=600, interval=5.0):
        """
        :param node_id: ID of the node to wait for.
        :type node_id: ``int``

        :param timeout: Timeout (in seconds).
        :type timeout: ``int``

        :param interval: How long to wait (in seconds) between each attempt.
        :type interval: ``float``

        :return: Node representing the newly built server
        :rtype: :class:`Node`
        """
        # poll until we get a node
        for i in range(0, timeout, int(interval)):
            try:
                node = self.ex_get_node(node_id)
                return node
            except HostVirtualException:
                time.sleep(interval)

        raise HostVirtualException(412, 'Timeout on getting node details')

    def _is_valid_fqdn(self, fqdn):
        if len(fqdn) > 255:
            raise HostVirtualException(
                500, "Need a valid FQDN (e.g, hostname.example.com)")
        if fqdn[-1] == ".":
            fqdn = fqdn[:-1]
        if re.match(HOSTNAME_RE, fqdn) is not None:
            return fqdn
        else:
            raise HostVirtualException(
                500, "Need a valid FQDN (e.g, hostname.example.com)")

    def _ensure_node_terminated(self, node, timeout=600):
        """Ensure the node is not installed,
        uninstall it if it is installed
        """
        # log where we are
        self.local_logger('info',
                          'We are inside the _ensure_node_terminated method')

        # default return values
        changed = False

        # uninstall the node if it is not showing up as terminated.
        if node.state != 'terminated':
            # uninstall the node
            try:
                if self.node_exists(node.id):
                    delete_job = self.ex_delete_node(node)
                if not bool(delete_job._job):
                    raise Exception("ex_delete_node returned nothing")
            except Exception as e:
                raise HostVirtualException(
                    500,
                    "ex_delete_node failed for node({0}) with error {1}"
                    .format(node.name, str(e)))

            # wait for the node to say it's terminated
            node = self._wait_for_state(
                node, 'terminated', job=delete_job, timeout=timeout)
            changed = True
        return changed, node

    def _ensure_node_stopped(self, node, timeout=600):
        """Called when we want to just make sure that a node is
        NOT running
        """
        # log where we are
        self.local_logger('info',
                          'We are inside the _ensure_node_stopped method')

        changed = False
        if node.state != 'stopped':
            stop_job = self.ex_stop_node(node)
            if (getattr(stop_job, '_job', None) is None):
                changed = False
                print(stop_job.__dict__, '\n', type(stop_job))
            # if not bool(stop_job._job):
            #    raise HostVirtualException(
            #        500,
            #        "Seems we had trouble stopping the node {0}"
            #        .format(node.name))
            else:
                # wait for the node to say it's stopped.
                node = self._wait_for_state(
                    node, 'stopped', job=stop_job, timeout=timeout)
                changed = True
        return changed, node

    def _ensure_node_unlinked(self, node, timeout=600):
        """Called when we want to ensure the node has been unlinked
        This will unlink it if it exists.
        """
        # log where we are
        self.local_logger(
            level='info',
            format='We are inside the _ensure_node_unlinked method')

        changed = False
        if self.node_exists(node.id):
            # Node exists, it must be terminated first though
            # first get a fresh copy to ensure we are using
            # the correct state
            node = self.ex_get_node(node.id)
            if node.state != 'terminated':
                # node is installed, terminate first
                changed, node = self._ensure_node_terminated(
                    node,
                    timeout=timeout)

            if node.state != 'terminated':
                raise HostVirtualException(
                    500,
                    "I tried to terminate the node {0}/{1} "
                    "so I could unlink it but it seems to "
                    "have not worked. Please troubleshoot"
                    .format(node.name, node.id))

            # it's terminated so we can unlink it
            unlinked = self.ex_unlink_package(node)

            # if we don't get a positive response we need to bail
            if not unlinked:
                raise HostVirtualException(
                    500,
                    "Seems we had trouble calling unlink "
                    "for node {0}/{1}"
                    .format(node.name, node.id))
            else:
                # Seems our command executed successfully
                # so wait for it to be gone
                node = self._wait_for_unlinked(
                    node, timeout=timeout)
                if node.state == 'unlinked':
                    changed = True
        return changed, node

    def _ensure_node_running(self, node, timeout=600):
        """Called when we want to just make sure the node is running
        This will build it if it has enough information
        """
        # log where we are
        self.local_logger(
            level='info',
            format='We are inside the _ensure_node_running method')

        changed = False
        if node.state != 'running':
            self.local_logger(
                level='info',
                format='We dont seem to be running')
            # first build the node if it is in state 'terminated'
            if node.state in ['terminated', 'unlinked']:
                # build it
                self.local_logger(
                    level='info',
                    format='we seem to be terminated or unlinked, building.')
                running_job = self.ex_provision_node(node=node)
            else:
                self.local_logger(
                    level='info',
                    format='We are just down, starting up')
                # node is installed so boot it up.
                running_job = self.ex_start_node(node)

            # if we don't get a positive response we need to bail
            if not bool(running_job._job):
                raise HostVirtualException(
                    500,
                    "Seems we had trouble building/starting node {0}"
                    .format(node.name))
            else:
                # Seems our command executed successfully
                if node.state in ['terminated', 'unlinked']:
                    # we are doing full install, wait for actual node
                    # before waiting for state...
                    node = self._wait_for_node(
                        node.id, timeout=timeout)

                # node should be installed started, check for 'running'
                node = self._wait_for_state(
                    node, 'running', job=running_job, timeout=timeout)
                changed = True
        return changed, node
