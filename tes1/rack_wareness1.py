#!/usr/bin/ python

import  netaddr
import  sys
sys.argv.pop(0)


netmask = '255.255.255.0'

for ip in sys.argv:
    address = '{0}/{1}'.format(ip, netmask)
    try:
        network_address = netaddr.IPNetwork(address).network
        print "/{0}".format(network_address)
    except:
        print "/rack-unknown"
