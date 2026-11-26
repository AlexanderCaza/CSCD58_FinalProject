To build the code:
make

To build mininet topo:
sudo python3 topo.py

To launch the node terminals:
inside mininet CLI:
xterm b1 c1 c2 c3

To boot the nodes:
In b1 cli: ./broker_node b1-eth0

In c1 cli: ./client_node c1-eth0 1

In c2 cli: ./client_node c2-eth0 2

In c3 cli: ./client_node c3-eth0 3

To subscribe, publish, and unsubscribe on a client:
sub 1
publish 1 hello
unsub 1

To inject bit losses:
In mininet CLI:
b1 tc qdisc replace dev b1-eth0 root netem corrupt 5%
c1 tc qdisc replace dev c1-eth0 root netem loss 15%