import zmq
import threading
import time
import random

# Configurações
DISCOVERY_PORT = 6666  # Porta de descoberta inicial
RESPONSE_BASE_PORT = 6660  # Base para portas de resposta
DISCOVERY_INTERVAL = 5  # Intervalo de descoberta em segundos

# Função para enviar pedidos de descoberta
def send_discovery_request(push_socket, my_endpoint):
    while True:
        print(f"[INFO] Enviando pedido de descoberta... (Meu endpoint: {my_endpoint})")
        push_socket.send_string(f"DISCOVERY_REQUEST::{my_endpoint}")
        time.sleep(DISCOVERY_INTERVAL)

# Função para receber pedidos de descoberta e responder com o endpoint
def receive_discovery_request(pull_socket, response_socket, my_endpoint):
    while True:
        message = pull_socket.recv_string()
        print(f"[INFO] Pedido de descoberta recebido: {message}")
        
        # Processa pedidos de descoberta
        if message.startswith("DISCOVERY_REQUEST::"):
            # Extrai o endpoint do nó descobridor
            discoverer_endpoint = message.split("::")[1]
            print(f"[INFO] Respondendo para {discoverer_endpoint} com meu endpoint: {my_endpoint}")
            response_socket.send_string(f"DISCOVERY_RESPONSE::{my_endpoint}")

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
    
    # Socket para enviar pedidos de descoberta (PUSH)
    push_socket = context.socket(zmq.PUSH)
    push_socket.bind(f"tcp://*:{DISCOVERY_PORT}")
    
    # Socket para receber pedidos de descoberta (PULL)
    pull_socket = context.socket(zmq.PULL)
    pull_socket.connect(f"tcp://localhost:{DISCOVERY_PORT}")
    
    # Socket para enviar respostas de descoberta (PUSH)
    response_socket = context.socket(zmq.PUSH)
    response_socket.bind(my_endpoint)
    
    # Socket para receber respostas de descoberta (PULL)
    pull_response_socket = context.socket(zmq.PULL)
    pull_response_socket.connect(my_endpoint)
    

    # Thread para enviar pedidos de descoberta periodicamente
    discovery_thread = threading.Thread(target=send_discovery_request, args=(push_socket, my_endpoint,))
    discovery_thread.daemon = True
    discovery_thread.start()

    # Thread para ouvir pedidos de descoberta e responder
    receive_thread = threading.Thread(target=receive_discovery_request, args=(pull_socket, response_socket, my_endpoint,))
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
