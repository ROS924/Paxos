import socket
import zmq


# Declare global variables used accross modules

broadcast_address = '192.168.15.55' # Broadcast address of your subnet

port_b = 6666  # Proxy UDP port number used for broadcast connections
portN1 = 5555  # node1 TCP port number
portN2 = 4444  # node2 TCP port number
portN3 = 3333  # node3 TCP port number
portN4 = 7777  # node4 TCP port number
portN5 = 8888  # node4 TCP port number
portN6 = 9999  # node4 TCP port number

nPorts=[portN1, portN2, portN3, portN4, portN5, portN6]

def start_server(port: int, known_nodes:list):
    # Create PULL socket (1 socket) (use bind since it will receive messages from N nodes)
    host = '0.0.0.0'  # Listen on all available interfaces
    context = zmq.Context()
    socket_pull = context.socket(zmq.PULL)
    socket_pull.bind(f"tcp://{host}:{port}")     #propria porta

# Create PUSH sockets (N sockets) (use connect since they will be used to send 1 message)
push_sockets_dict = {}

for node in range(nPorts):
    socket_push = context.socket(zmq.PUSH)
    socket_push.connect(f"tcp://{addrs[node]}:{nPorts[node]}")  #Portas dos outros nós
    push_sockets_dict[node] = socket_push

