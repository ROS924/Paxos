import sys
import os
import random
import threading
from multiprocessing import Process, Barrier
import zmq
import time
import numpy

PORTA = 6666

# Envia mensagem 
def sendRegular(body, sender_id, target_id, push_socket, prob):
    if prob != 0:
        is_crashed = numpy.random.choice([True, False], p=[prob, 1 - prob])
    else:
        is_crashed = False
        
    message = {}
    if is_crashed:
        message = {"body": f"CRASH {sender_id}", "from": sender_id, "to": target_id}
    else:
        message = {"body": body, "from": sender_id, "to": target_id}

    push_socket.send_json(message)

# Envia Broadcast 
def broadcast(body, sender_id,numNodes, push_sockets_dict, prob, am_i_excluded=False):
    for target_id in range(numNodes):
        push_socket = push_sockets_dict[target_id]

        if am_i_excluded and target_id == sender_id:
            continue

        # send message to target
        sendRegular(body, sender_id, target_id, push_socket, prob)


def PaxosNode(node_id, value, numNodes, prob, numRounds, barrier):
    maxVotedRound = [-1]*numNodes  # Id da Úlima proposta aceita
    maxVotedVal = [-1]*numNodes  # Valor da última proposta aceita
    proposeVal = -1  # Valor proposto
    decision = -1  # Valor já aceito

    # Socket para receber mensagens
    context = zmq.Context()
    socket_pull = context.socket(zmq.PULL)
    socket_pull.bind(f"tcp://127.0.0.1:{PORTA + node_id}")

    # Socket para enviar mensagens
    push_sockets_dict = {}

    for target_id in range(numNodes):
        socket_push = context.socket(zmq.PUSH)
        socket_push.connect(f"tcp://127.0.0.1:{PORTA + target_id}")
        push_sockets_dict[target_id] = socket_push

    #Aguarda todos os nós se conectarem
    time.sleep(0.3)

    # Execução do algoritmo
    for r in range(numRounds):

        is_proposer = r % numNodes == node_id

        if is_proposer:
            print(f"\nRodada {r} proposta com valor inicial: {value}")
            # Broadcast 'PREPARE'

            time.sleep(0.3)

            broadcast(
                body= f"PREPARE {r}",
                sender_id=node_id,
                numNodes=numNodes,
                prob=prob,
                push_sockets_dict=push_sockets_dict,
            )

        
        message_received = socket_pull.recv_json()

        
        message_received_body = message_received["body"]
        message_received_from = message_received["from"]
        message_received_to = message_received["to"]

        time.sleep(0.3)

        # Fase 1 --------------------------------------------------
        promise_count = 0
        will_propose = False

        if is_proposer:
            # Atua como proponente
            print(
                f"Propositor {node_id} enviou e recebeu na fase de preparação: {message_received_body}"
            )

            
            is_received_start = False

            if "PREPARE" in message_received_body:
                promise_count += 1
                is_received_start = True

            # Recebe mensagem dos aceitadores ('PROMISE|CRASH')

            received_maxVotedRound = -1
            received_maxVotedVal = -1

            for _ in range(numNodes - 1):
                message_received = socket_pull.recv_json()

                
                message_received_body = message_received["body"]
                message_received_from = message_received["from"]
                message_received_to = message_received["to"]

                print(
                    f"Propositor {node_id} recebeu de {message_received_from} na fase de preparação: {message_received_body}"
                )

                if "PROMISE" in message_received_body:
                    promise_count += 1

                    # Recebendo promessa "PROMISE {maxVotedRound} {maxVotedVal}"
                    parsed_join = message_received_body.split(" ")


                    # Compara os valores dos IDs das proposta e escolhe o maior como sua promessa
                    if int(parsed_join[1]) > received_maxVotedRound and len(parsed_join) >= 3:
                        received_maxVotedRound = int(parsed_join[1])
                        received_maxVotedVal = int(parsed_join[4])

            # Quando a maioria dos nós promete ao propenente
            if promise_count > int(numNodes / 2):
                will_propose = True

                # Aceita o próprio valor proposto
                if is_received_start:
                    if maxVotedRound[node_id] == -1:
                        proposeVal = value
                    else:
                        proposeVal = received_maxVotedVal

                # Aceita o valor que foi aceito pelos outros nós que não foi proposto por si próprio
                else:
                    proposeVal = value

            else:
                will_propose = False

        elif not is_proposer:
            # Atua como aceitador
            print(f"Aceitador {node_id} recebeu na fase de preparação: {message_received_body}")

            if "PREPARE" in message_received_body:

                time.sleep(0.3)
                idU = message_received_body.split(" ")[1]
                # Envia promessa ao proponente
                if maxVotedRound[node_id] == -1:
                    sendRegular(
                        body=f"PROMISE {r}",
                        sender_id=node_id,
                        target_id=message_received_from,
                        prob=prob,
                        push_socket=push_sockets_dict[message_received_from],
                    )
                else:
                    sendRegular(
                        body=f"PROMISE {r} accepted {maxVotedRound[node_id]}, {maxVotedVal[node_id]} ",
                        sender_id=node_id,
                        target_id=message_received_from,
                        prob=prob,
                        push_socket=push_sockets_dict[message_received_from],
                    )

            elif "CRASH" in message_received_body:
                # Responde com falha ao receber uma falha
                sendRegular(
                    body=f"CRASH {node_id}",
                    sender_id=node_id,
                    target_id=message_received_from,
                    push_socket=push_sockets_dict[message_received_from],
                    prob=0
                )

        barrier.wait()
        # Fase 2 --------------------------------------------------

        if is_proposer:
            # Atua como proponente
            time.sleep(0.3)

            if will_propose:
                # Broadcast 'ACCEPT_REQUEST'
                broadcast(
                    body=f"ACCEPT_REQUEST {r} {proposeVal}",
                    sender_id=node_id,
                    numNodes=numNodes,
                    prob=prob,
                    push_sockets_dict=push_sockets_dict,
                )
            else:
                # Broadcast 'ROUNDCHANGE'
                print(f"Propositor do Round {r} cancelou o round por falta de quorum, valor se mantém: {proposeVal} ")
                broadcast(
                    body="ROUNDCHANGE",
                    sender_id=node_id,
                    numNodes=numNodes,
                    push_sockets_dict=push_sockets_dict,
                    prob=0
                )
                # Muada de rodade

        # Recebe mensagem do proponente
        message_received = socket_pull.recv_json()

        message_received_body = message_received["body"]
        message_received_from = message_received["from"]
        message_received_to = message_received["to"]

        if is_proposer:
            # Atua como proponente
            if will_propose:
                # Envia o accept request e espera o accepted
                vote_count = 0
                is_received_propose = False

                if "ROUNDCHANGE" not in message_received_body:
                    print(
                        f"Propositor {node_id} enviou e recebeu na fase de aceitação: {message_received_body}"
                    )

                    if "ACCEPT_REQUEST" in message_received_body:
                        vote_count += 1
                        is_received_propose = True

                    elif "CRASH" in message_received_body:
                        # TODO
                        pass

                    # Receive responses from N-1 acceptors ('PROMISE|CRASH')

                    for _ in range(numNodes - 1):
                        message_received = socket_pull.recv_json()

                        # Parse message received
                        message_received_body = message_received["body"]
                        message_received_from = message_received["from"]
                        message_received_to = message_received["to"]

                        print(
                            f"Propositor {node_id} recebeu de {message_received_from} na fase de aceitação: {message_received_body}"
                        )

                        if "ACCEPT" in message_received_body:
                            vote_count += 1

                    if is_received_propose:
                        maxVotedRound[node_id] = r
                        maxVotedVal[node_id] = proposeVal

                    if vote_count > int(numNodes / 2):
                        decision = proposeVal
                        print(f"Foi decidido o valor: {decision} para a rodada {r}")
                    else:
                       print(f"Mantém-se o valor decidido anteriormente: {proposeVal} para a rodada {r}") 

            pass

        elif not is_proposer:
            # Atua como aceitador
            time.sleep(0.3)
            print(f"Aceitador {node_id} recebeu na fase de aceitação: {message_received_body}")


            if "ACCEPT_REQUEST" in message_received_body:
                # Envia accepted
                valorRecebido = message_received_body.split(" ")[2]
                sendRegular(
                    body=f"ACCEPT {r} {valorRecebido}",
                    sender_id=node_id,
                    target_id=message_received_from,
                    prob=prob,
                    push_socket=push_sockets_dict[message_received_from],
                )
                maxVotedRound[node_id] = r
                maxVotedVal[node_id] = int(message_received_body.split(" ")[2])

            elif "ROUNDCHANGE" in message_received_body:
                pass
            elif "CRASH" in message_received_body:
                # Envia uma falha ao receber uma falha
                sendRegular(
                    body=f"CRASH {node_id}",
                    sender_id=node_id,
                    target_id=message_received_from,
                    push_socket=push_sockets_dict[message_received_from],
                    prob=0
                )

            pass

        barrier.wait()
        time.sleep(0.3)
        # Vai para próxima rodada
    pass


def main(args):
    numNodes = int(args[1])
    prob = float(args[2])
    numRounds = int(args[3])

    barrier = Barrier(numNodes)

    print(f"Numero de nodes: {numNodes}, Probabilidade de crash: {prob}, Numero de rodadas: {numRounds}")

    # Cria processos como nós do paxos
    processes = []

    for node_id in range(numNodes):
        value = random.randint(0, 10)
        process = Process(
            target=PaxosNode,
            args=(
                node_id,
                value,
                numNodes,
                prob,
                numRounds,
                barrier,
            ),
        )
        processes.append(process)

    for process in processes:
        process.start()

    # Espera todos os nós terminarem suas rodadas
    for process in processes:
        process.join()

    pass


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Os argumentos são (Numero de nodes, Probabilidade de crash, Numero de rodadas)")
    else:
        main(args=sys.argv)