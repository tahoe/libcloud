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
    """Reads in key_file to populate NodeAuthSSHKey object

    Example key_file
    /home/user/.ssh/id_rsa.pub

    :param key_file: Path to public key file to use.\
    :type key_file: ``str``

    :rtype: :class:`NodeAuthSSHKey`

    """
    with open(key_file) as f:
        key = f.read()
        auth = NodeAuthSSHKey(pubkey=key)
    return auth


class HostVirtualNodeDriver(NodeDriver):
    """Version selector driver

    This driver is the main driver to be used since
    it is here to allow one to select which API version to use


    :return: A newly created NodeDriver for a given API version
    :rtype: :class:`HostVirtualNodeDriver_v1`
        or :class:`HostVirtualNodeDriver_v2`

    """
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
    """This is the old version of our driver

    It has been patched a bit to fix some bugs.


    """
    type = Provider.HOSTVIRTUAL
    name = 'HostVirtual (API V1.0)'
    connectionCls = HostVirtualComputeConnection
    features = {'create_node': ['ssh_key', 'password']}
    API_ROOT = API_VARS['v1']['API_ROOT']

    def __init__(self, key, secure=True, host=None,
                 port=None, api_version='v1'):
        """
        :param  key: Users API key
        :type   key: ``str``

        :keyword    secure: Whether to use SSL (default True)
        :type       secure: ``bool``

        :keyword    host: Override connection host for API (default None)
        :type       host: ``str``

        :keyword    port: Override connection port for API (default None)
        :type       port: ``int``

        :keyword    api_version: For selecting the right subclass
        :type       api_version: ``str``
        """
        self.location = None
        super(HostVirtualNodeDriver, self).__init__(
            key=key,
            secure=secure,
            host=host, port=port
        )

    def list_nodes(self):
        """List my Nodes

        :return: List of my Nodes
        :rtype: `list` of :class:`Node`
        """
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
        """List plan sizes

        Possibly filter by location

        :keyword    location: Location ID to get list from (optional)
        :type       location: `int`

        :return: List of plan sizes
        :rtype: `list` of :class:`NodeSize`
        """
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
        """List locations available for building

        :return: List of locations to build a domU
        :rtype: `list` of :class:`NodeLocation`
        """
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

    def create_node(self, name, image, size, location=None, auth=None):
        """Creates a node

        Example of node creation with ssh key deployed:

        :param  name: FQDN of domU (required)
        :type   name: ``str``

        :param  size: Package instance (required)
        :type   size: Instance of :class:`NodeSize`

        :param  image: OS Image to install (required)
        :type   image: Instance of :class:`NodeImage`

        :param  location: Location to install the Node
        :type   location: Instance of :class:`NodeLocation`

        :keyword location: Location to install the Node (optional)
        :type location: Instance of :class:`NodeLocation`

        :keyword auth: Authentication to use for Node (optional)
        :type auth: Instance of :class:`NodeAuthSSHKey`
            or :class:.NodeAuthPassword`

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

        auth = self._get_and_check_auth(auth)

        if not self._is_valid_fqdn(name):
            raise HostVirtualException(
                500, "Name should be a valid FQDN (e.g, hostname.example.com)")

        # simply order a package first
        pkg = self.ex_order_package(size)

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
        self.ex_provision_node(node=stub_node, auth=auth)
        node = self._wait_for_node(stub_node.id)
        if getattr(auth, 'generated', False):
            node.extra['password'] = auth.password

        return node

    def reboot_node(self, node):
        """Reboot a node

        Reboots a passed in Node object

        :param  node: Node to reboot
        :type   node: Instance of :class:`Node`

        :return: True if result is a success, False otherwise
        :rtype: ``bool``
        """
        mbpkgid = node.id
        result = self.connection.request(
            '{0}/cloud/server/reboot/{1}'
            .format(self.API_ROOT, mbpkgid),
            method='POST').object

        return bool(result)

    def destroy_node(self, node):
        """Destroy a node

        Destroys a passed in Node object

        :param  node: Node to destroy
        :type   node: Instance of :class:`Node`

        :return: True if result is a success, False otherwise
        :rtype: ``bool``
        """
        mbpkgid = node.id
        result = self.connection.request(
            '{0}/cloud/cancel/{1}'
            .format(self.API_ROOT, mbpkgid),
            method='POST').object

        return bool(result)

    def list_images(self):
        """List OS images available

        :return: List of NodeImage objects
        :rtype: `list` of :class:`NodeImage`
        """
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
        """List the server packages

        :return: List of package json dicts
        :rtype: `list` of ``dict`` containting package info
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
        """Order a server package.

        :param size: Size of package
        :type  size: Instance of :class:`NodeSize`

        :return: Dict with size info
        :rtype: ``dict``
        """
        plan = size.name
        return self.connection.request(
            '{0}/cloud/buy/{1}'
            .format(self.API_ROOT, plan),
            method='POST'
        ).object

    def ex_cancel_package(self, node):
        """Cancel a server package.

        :param  node: Node which should be used
        :type   node: class:`Node`

        :return: Dict with cancel return data
        :rtype: ``dict``
        """

        result = self.connection.request(
            '{0}/cloud/cancel/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return result

    def ex_unlink_package(self, node):
        """Unlink a server package from location.

        :param  node: Node which should be unlinked
        :type   node: class:`Node`

        :return: Dict with unlink return data
        :rtype: ``dict``
        """

        result = self.connection.request(
            '{0}/cloud/unlink/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return result

    def ex_get_node(self, node_id):
        """Get a single node.

        :param  node_id: Id of node to get
        :type   node_id: ``int``

        :return: Node
        :rtype: Instance of :class:`Node`
        """

        result = self.connection.request(
            '{0}/cloud/server/{1}'
            .format(self.API_ROOT, node_id)
        ).object
        node = self._to_node(result)
        return node

    def ex_stop_node(self, node):
        """Stop a node.

        :param  node: Node which should be used
        :type   node: class:`Node`

        :return: Dict with stop return data
        :rtype: ``dict``
        """
        result = self.connection.request(
            '{0}/cloud/server/shutdown/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return bool(result)

    def ex_start_node(self, node):
        """Start a node.

        :param  node: Node which should be used
        :type   node: class:`Node`

        :return: True if success otherwise False
        :rtype: ``bool``
        """
        result = self.connection.request(
            '{0}/cloud/server/start/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return bool(result)

    def ex_provision_node(self, **kwargs):
        """Provision a server on a VR package and get it booted

        Note: If a node fails to build, it will be canceled automatically

        :keyword    node: node which should be used

        :param      **kwargs:

        :return: True if node is built, otherwise False
        :rtype: ``bool``
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
        """Delete a node.

        :param  node: Node which should be used
        :type   node: class:`Node`

        :return: True if result is a success, False otherwise
        :rtype: ``bool``
        """

        result = self.connection.request(
            '{0}/cloud/server/delete/{1}'
            .format(self.API_ROOT, node.id),
            method='POST'
        ).object

        return bool(result)

    def _to_node(self, data):
        """Build a node from data provided

        :param  data: Dict of Node info
        :type   data: ``dict``

        :return: Returns a Node object
        :rtype: :class:`Node`
        """
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
        """Wait for a Node to be created

        :param  node_id: ID of the node to wait for.
        :type   node_id: int``

        :param  timeout: Timeout (in seconds). (Default value = 30)
        :type   timeout: int``

        :param  interval: How long to wait (in seconds) between each attempt.
            (Default value = 5.0)
        :type   interval: float``

        :return: Node representing the newly built server
        :rtype: class:`Node`

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
        """Check if an fqdn is valid

        :param  fqdn: hostname to check
        :type   fqdn: ``str``

        :return: fqdn or exception
        :rtype: ``str`` or :class:`HostVirtualException`
        """
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
    """New driver for new NetActuate platform

    This driver adds a bunch of functionality along with providing
    the same interface as the old driver.
    """
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
        """
        :param  key: Users API key
        :type   key: ``str``

        :keyword    secure: Whether to use SSL (default True)
        :type       secure: ``bool``

        :keyword    host: Override connection host for API (default None)
        :type       host: ``str``

        :keyword    port: Override connection port for API (default None)
        :type       port: ``int``

        :keyword    logger: Logger to use for debugging/logging
            (default :function:`dummyLogger`)
        :type       logger: ``func``

        :keyword    debug: turn on debug logging (default False)
        :type       debug: ``bool``

        :keyword    auth: Authentication method to use (default None)
        :type       auth: :class:`NodeAuthSSHKey` or :class:`NodeAuthPassword`

        :keyword    ssh_key_file: File path to get ssh key from
            (default None)
        :type       ssh_key_file: ``str``

        :keyword    ssh_user: User to ssh as (default "root")
        :type       ssh_user: ``str``

        :keyword    ssh_port: Port to ssh over (default 22)
        :type       ssh_port: ``str``

        :keyword    user_password: Password to use if no ssh key
        :type       user_password: ``str``

        :keyword    ssh_agent: Whether or not to use an ssh agent to auth.
            (default False)
        :type       ssh_agent: ``bool``

        :keyword    api_version: For selecting the right subclass
        :type       api_version: ``str``
        """
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
        """List my Nodes

        :return: List of my Nodes
        :rtype: `list` of :class:`NetActuateNode`
        """
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
        """List locations available for building

        :return: List of locations to build a domU
        :rtype: `list` of :class:`NodeLocation`
        """
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
        """List plan sizes

        Possibly filter by location

        :keyword    location: Location ID to get list from (optional)
        :type       location: `int`

        :return: List of plan sizes
        :rtype: `list` of :class:`NodeSize`
        """
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
        """List OS images available

        :return: List of NodeImage objects
        :rtype: `list` of :class:`NodeImage`
        """
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
        """Creates a node

        Example of node creation with ssh key deployed:

        :param  name: FQDN of domU (required)
        :type   name: ``str``

        :param  size: Package instance (required)
        :type   size: Instance of :class:`NodeSize`

        :param  image: OS Image to install (required)
        :type   image: Instance of :class:`NodeImage`

        :param  location: Location to install the Node
        :type   location: Instance of :class:`NodeLocation`

        :keyword location: Location to install the Node (optional)
        :type location: Instance of :class:`NodeLocation`

        :keyword auth: Authentication to use for Node (optional)
        :type auth: Instance of :class:`NodeAuthSSHKey`
            or :class:.NodeAuthPassword`

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

        :param node: param want_state:  (Default value = "running")
        :param timeout: Default value = 600)
        :param want_state:  (Default value = "running")

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
        """List the server packages."""

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
        """Order a server package.

        :param size: type       node: :class:`NodeSize`

        """

        plan = size.name
        pkg = self.connection.request(
            '{0}/cloud/buy/{1}'.format(self.API_ROOT, plan), method='POST'
        ).object

        return pkg

    def ex_cancel_package(self, node):
        """Cancel a server package.

        :param node: Node which should be used
        :type node: class:`Node`

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
        """Unlink a server package from location.

        :param node: Node which should be used
        :type node: class:`Node`

        """

        mbpkgid = node.id
        result = self.connection.request(
            '{0}/cloud/server/unlink/{1}'.format(self.API_ROOT, mbpkgid),
            method='POST'
        ).object

        return bool(result)

    def node_exists(self, node):
        """Check for Node existence

        :param node: Node to check for existence
        :type node: :class:`NetActuateNode`

        :return: True if node exists, False otherwise
        :rtype: ``bool``
        """
        exists = True
        try:
            self.ex_get_node(node.id)
        except Exception:
            exists = False
        return exists

    def ex_get_node(self, node_id):
        """Get a single node.

        :param node_id: id of the node to retrieve
        :type node_id: str``

        :return: Instance of a NetActuateNode
        :rtype: :class:`NetActuateNode`
        """
        result = self.connection.request(
            '{0}/cloud/server/{1}'.format(self.API_ROOT, node_id)).object
        if isinstance(result, list) and len(result) > 0:
            node = self._to_node(result[0])
        elif isinstance(result, dict):
            node = self._to_node(result)
        return node

    def ex_stop_node(self, node):
        """Stop a node.

        :param node: Node which should be used
        :type node: class:`NetActuateNode`

        :return: Instance of NetActuateJobStatus for checking status
        :rtype: :class:`NetActuateJobStatus`
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
        """Start a node.

        :param node: Node which should be used
        :type node: class:`NetActuateNode`

        :return: Instance of NetActuateJobStatus for checking status
        :rtype: :class:`NetActuateJobStatus`
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
        """Reboot a Node

        :param node:  Node to be rebooted
        :type node: :class:`NetActuateNode`

        :return: Instance of NetActuateJobStatus for checking status
        :rtype: :class:`NetActuateJobStatus`
        """
        result = self.connection.request(
            '{0}/cloud/server/reboot/{1}'
            .format(self.API_ROOT, node.id),
            method='POST').object

        return NetActuateJobStatus(
            conn=self.connection,
            node_id=node.id,
            job_result=result)

    def ex_provision_node(self, **kwargs):
        """Provision a server on a VR package and get it booted

        :keyword node: node which should be used

        :return: Node representing the newly built server
        :rtype: class:`Node`

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
        """Delete a node.

        :param node: Node which should be used
        :type node: class:`Node`

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

        :param avail_locs: Default value = [])
        :param want_location: Default value = None)

        """
        location = None
        loc_possible_list = [
            loc for loc in avail_locs if loc.name == want_location or loc.id == want_location
        ]

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

        :param avail_oses: Default value = [])
        :param want_os: Default value = None)

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
        """Turn API response to a NetActuateNode object

        :param data: A dictionary representation of a NetActuateNode
        :type data: ``dict``

        :return: Instance of a NetActuateNode
        :rtype: :class:`NetActuateNode`
        """
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

        :param node: obj     Node object
        :param timeout: int     timeout in seconds (Default value = 600)
        :param interval: float   sleep time between loops (Default value = 10)

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

        :param node: obj     Node object
        :param timeout: int     timeout in seconds (Default value = 600)
        :param interval: float   sleep time between loops (Default value = 10)
        :param want_state: string  string of the desired state
        :param job: Default value = None)

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
        :type node_id: int``
        :param timeout: Timeout (in seconds). (Default value = 600)
        :type timeout: int``
        :param interval: How long to wait (in seconds) between each attempt.
            (Default value = 5.0)
        :type interval: float``
        :return: Node representing the newly built server
        :rtype: class:`Node`

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
        """Check if FQDN is valid

        :param fqdn: Hostname to check
        :type fqdn: ``str``

        :return: fqdn or exception
        :rtype: ``str`` or :class:`HostVirtualException`
        """
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

        :param node: param timeout:  (Default value = 600)
        :param timeout:  (Default value = 600)

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

        :param node: param timeout:  (Default value = 600)
        :param timeout:  (Default value = 600)

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

        :param node: param timeout:  (Default value = 600)
        :param timeout:  (Default value = 600)

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

        :param node: param timeout:  (Default value = 600)
        :param timeout:  (Default value = 600)

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
