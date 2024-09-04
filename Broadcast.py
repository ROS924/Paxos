import zmq
import threading
import time
import random

# Configurações
DISCOVERY_PORT = 6666  # Porta de descoberta inicial
RESPONSE_BASE_PORT = 6660  # Base para portas de resposta
DISCOVERY_INTERVAL = 5  # Intervalo de descoberta em segundos

# Função para enviar pedidos de descoberta
def send_discovery_request(known_nodes:list,context:zmq.Context):
    #while True:
        # Socket para enviar pedidos de descoberta (PUSH)
        push_socket = context.socket(zmq.PUSH)
        push_socket.bind(f"tcp://*:{DISCOVERY_PORT}")

        print(f"[INFO] Enviando pedido de descoberta... (Meu endpoint: {known_nodes[0]})")
        push_socket.send_string(f"DISCOVERY_REQUEST::{known_nodes}")
        # DISCOVERY_REQUEST:: [node1,node2,node3,etc]
        push_socket.close()
        #time.sleep(DISCOVERY_INTERVAL)

# Função para receber pedidos de descoberta e responder com o endpoint
def receive_discovery_request(pull_socket, response_socket, known_nodes,context:zmq.Context):
    

    while True:

        pedidosRecebidos = 0
        comecouOuvir = time.time()
        ouvindoPor = 0

        while (ouvindoPor < 5 or pedidosRecebidos != 0):

            print("Cmeçou a ouvir")    

            if(pull_socket.recv_string()):
                pedidosRecebidos += 1

            print(f"ouvindo {pull_socket.last_endpoint}")

            message = pull_socket.recv_string()
            print(f"[INFO] Pedido de descoberta recebido: {message}")
            
            # Processa pedidos de descoberta
            if message.startswith("DISCOVERY_REQUEST::"):
                # Extrai o endpoint do nó descobridor
                discoverer_endpoint = message.split("::")[1]
                list(discoverer_endpoint)
                for node in discoverer_endpoint:
                    print(f"[INFO] Respondendo para {node} com meu endpoint: {known_nodes[0]}")
                    response_socket.send_string(f"DISCOVERY_RESPONSE::{known_nodes[0]}")

            ouvindoAgora = time.time()
            ouvindoPor = ouvindoAgora - comecouOuvir
            print(ouvindoPor)

        '''# Thread para enviar pedidos de descoberta periodicamente
        discovery_thread = threading.Thread(target=send_discovery_request, args=(push_socket, my_endpoint,context))
        discovery_thread.daemon = True
        discovery_thread.start()'''

        send_discovery_request(known_nodes,context)

# Função para receber respostas de outros nós
def listen_for_responses(pull_response_socket, known_nodes:list):
    while True:
        message = pull_response_socket.recv_string()
        print(f"[INFO] Resposta recebida: {message}")
        
        # Processa respostas de descoberta
        if message.startswith("DISCOVERY_RESPONSE::"):
            # Extrai o endpoint do nó respondente
            endpoint = message.split("::")[1]
            if endpoint not in known_nodes:
                known_nodes.append(endpoint)
                print(f"[INFO] Novo nó descoberto: {endpoint}")


def doBroadcast(known_nodes:list, my_port):
    # Cria contexto e sockets ZeroMQ
    context = zmq.Context()
    
    # Configura um endpoint único para cada nó
    my_endpoint = f"tcp://localhost:{my_port}"  # Porta única para cada nó

    known_nodes.append(my_endpoint)
    
    
    
    # Socket para receber pedidos de descoberta (PULL)
    pull_socket = context.socket(zmq.PULL)
    pull_socket.connect(f"tcp://localhost:{DISCOVERY_PORT}")
    
    # Socket para enviar respostas de descoberta (PUSH)
    response_socket = context.socket(zmq.PUSH)
    response_socket.bind(my_endpoint)
    
    # Socket para receber respostas de descoberta (PULL)
    pull_response_socket = context.socket(zmq.PULL)
    pull_response_socket.connect(my_endpoint)
    

    

    # Thread para ouvir pedidos de descoberta e responder
    receive_thread = threading.Thread(target=receive_discovery_request, args=(pull_socket, response_socket, known_nodes,context,))
    receive_thread.daemon = True
    receive_thread.start()
    
    # Thread para ouvir respostas de outros nós
    response_thread = threading.Thread(target=listen_for_responses, args=(pull_response_socket, known_nodes,))
    response_thread.daemon = True
    response_thread.start()

    # Mantém o programa rodando
    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[INFO] Finalizando...")
