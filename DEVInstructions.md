To build the code:
make

To build mininet topo:
sudo python3 topo.py

To launch the node terminals:
inside mininet CLI: 
xterm b1 c1 c2 c3

To boot the nodes:
In b1 cli: ./broker_node b1-eth0

In c1 cli: ./client_node c1-eth0 0x0001

In c2 cli: ./client_node c2-eth0 0x0002

In c3 cli: ./client_node c3-eth0 0x0003

To send messages on a client:
send hello

To inject bit losses:
In mininet CLI:
b1 tc qdisc change dev b1-eth0 root netem corrupt 5%
c1 tc qdisc change dev c1-eth0 root netem loss 15%