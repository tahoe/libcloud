import libcloud
from libcloud import security
security.VERIFY_SSL_CERT = False
cls=libcloud.get_driver(type=libcloud.DriverType.COMPUTE, provider=libcloud.compute.types.Provider.HOSTVIRTUAL)
with open('/home/dennis/api_key.txt', 'r') as f:
    api_key = f.read().rstrip()
conn=cls(api_key, api_version='1.0')
