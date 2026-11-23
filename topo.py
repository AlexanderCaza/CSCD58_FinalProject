from mininet.net import Mininet
from mininet.node import OVSSwitch
from mininet.link import TCLink
from mininet.cli import CLI
from mininet.log import setLogLevel

def run():
    # We use an OVSSwitch in standalone mode to emulate a wireless radio topology
    net = Mininet(switch=OVSSwitch, link=TCLink, controller=None)

    b1 = net.addHost('b1') # Broker node
    c1 = net.addHost('c1') # Client 1
    c2 = net.addHost('c2') # Client 2
    c3 = net.addHost('c3') # Client 3
    sw = net.addSwitch('sw1', failMode='standalone')

    # Connect all nodes to the switch with low-rate wireless link parameters
    # We can adjust loss to test our QoS retransmit and bandwidth of 0.25 Mbps is pretty 
    # true to life for radio links, but our packets are so small it doesn't actually matter
    # 5 ms with 1 ms jitter for delay is very high given the short distances and speed of light,
    # but that isn't very relevant. We give the broker itself 5 Mbps just to be extra true to the
    # better hardware it would have
    net.addLink(b1, sw, bw=5, delay='5ms', loss=0)
    net.addLink(c1, sw, bw=0.25, delay='5ms', jitter='1ms',  loss=0)
    net.addLink(c2, sw, bw=0.25, delay='5ms', jitter='1ms', loss=0)
    net.addLink(c3, sw, bw=0.25, delay='5ms', jitter='1ms', loss=0)

    net.start()
    CLI(net)
    net.stop()

if __name__ == '__main__':
    setLogLevel('info')
    run()