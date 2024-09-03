import sys
import socket, os, random, threading, zmq, time, numpy
#from multiprocessing import Process, Barrier
#from utils import *
from Broadcast import *


roundCounter = 0      # Contador de rodadas  
acceptedProposal:int  # Chave da proposta aceita
accPropoVal: str      # Valor da proposta aceita
known_nodes = []
my_port:int  



#---------------------------------------------------------------------------
#-------------------------CÓDIGO DO CARA------------------------------------
#---------------------------------------------------------------------------
# ver questao do crash

BASE_PORT = 5550

# Sends message with arg that permit probability
def sendRegular(body, sender_id, target_id, push_socket, prob):
    #see if need to instantiate
    if prob != 0:
        is_crashed = numpy.random.choice([True, False], p=[prob, 1 - prob])

    message = {}
    if is_crashed:
        message = {"body": f"CRASH {sender_id}", "from": sender_id, "to": target_id}
    else:
        message = {"body": body, "from": sender_id, "to": target_id}

    push_socket.send_json(message)

# Broadcast message without crash probability
def broadcastRegular(body, sender_id, push_sockets_dict, prob, am_i_excluded=False):
    for target_id in range(len(known_nodes)):
        push_socket = push_sockets_dict[target_id]

        if am_i_excluded and target_id == sender_id:
            continue

        # send message to target
        sendRegular(body, sender_id, target_id, push_socket, prob)



def PaxosNode(my_port, prob):
    proposalId = -1  # Proposer & Acceptor
    maxVotedVal = None  # Proposer & Acceptor
    proposeVal = None  # Only Proposer
    decision = None  # Only Proposer

    # Create PULL socket (1 socket) (use bind since it will receive messages from N nodes)
    context = zmq.Context()
    socket_pull = context.socket(zmq.PULL)
    socket_pull.bind(f"tcp://localhost:{my_port}")

    # Create PUSH sockets (N sockets) (use connect since they will be used to send 1 message)
    push_sockets_dict = {}

    for index in range(len(known_nodes)):
        socket_push = context.socket(zmq.PUSH)
        socket_push.connect(known_nodes[index])
        push_sockets_dict[index] = socket_push

    # Wait for everyone finishing establishing their connections
    time.sleep(0.3)

    # Run algorithm
    while True:

        is_proposer = random.randint(0,1)

        if bool(is_proposer):

            value = random.randint(0, 8)

            print(f"PROPOSER {my_port} in ROUND {proposalId} PROPOSED VALUE: {value}")
            # Broadcast 'PREPARE'

            time.sleep(0.3)

            broadcastRegular(
                "PREPARE",
                my_port,
                push_sockets_dict,
                prob,
            )

        # Receive 'PREPARE|CRASH' from proposer
        message_received = socket_pull.recv_json()

        # Parse message received
        message_received_body = message_received["body"]
        message_received_from = message_received["from"]
        message_received_to = message_received["to"]

        time.sleep(0.3)

        # Phase1 --------------------------------------------------
        promise_count = 0
        will_propose = False

        if is_proposer:
            print(
                f"PROPOSER OF {my_port} RECEIVED IN PREPARE PHASE: {message_received_body}"
            )

            # As a proposer:
            is_received_start = False

            if "PREPARE" in message_received_body:
                promise_count += 1
                is_received_start = True

            # Receive responses from N-1 acceptors ('PROMISE|CRASH')

            received_proposalId = -1
            received_maxVotedVal = -1

            for _ in range(len(known_nodes)- 1):
                message_received = socket_pull.recv_json()

                # Parse message received
                message_received_body = message_received["body"]
                message_received_from = message_received["from"]
                message_received_to = message_received["to"]

                print(
                    f"PROPOSER OF {my_port} RECEIVED IN PREPARE PHASE: {message_received_body}"
                )

                if "PROMISE" in message_received_body:
                    promise_count += 1

                    # Incoming message "PROMISE {proposalId} {maxVotedVal}"
                    parsed_promise = message_received_body.split(" ")

                    if int(parsed_promise[1]) > received_proposalId:
                        # If incoming PROMISE's maxVotedVal is bigger than previous
                        # Then update previous with incoming maxVotedVal, round too
                        # This is basically for picking the PROMISE message with biggest proposalId
                        # Then we can set proposeVal to this message's maxVotedVal
                        received_proposalId = int(parsed_promise[1])
                        received_maxVotedVal = int(parsed_promise[2])

            # If majority promiseed
            if promise_count > int(len(known_nodes) / 2):
                will_propose = True

                # If proposer received 'PREPARE' from itself in the beginning
                # And if proposalId is -1, then update proposeVal
                if is_received_start:
                    if proposalId == -1:
                        proposeVal = value
                    else:
                        # SUSPICIOUS
                        proposeVal = received_maxVotedVal

                # If proposer didn't receive 'PREPARE' from itself in the beginning,
                # Then set proposalId to the maximum proposalId came from PROMISEs
                # And set maxVotedVal to the maxixmum maxVotedVal came from PROMISEs
                else:
                    proposeVal = value

            # If majority didn't promise
            else:
                will_propose = False

        elif not is_proposer:
            # As an acceptor:
            print(f"ACCEPTOR {my_port} RECEIVED IN PREPARE PHASE: {message_received_body}")

            if "PREPARE" in message_received_body:

                time.sleep(0.3)

                # Send "PROMISE" to proposer
                sendRegular(
                f"PROMISE {proposalId} {maxVotedVal}",
                    my_port,
                    message_received_from,
                    push_sockets_dict[message_received_from],
                    prob,
                )

            elif "CRASH" in message_received_body:
                # If an acceptor receives 'CRASH', then it responds with 'CRASH' too
                sendRegular(f"CRASH {my_port}",
                    my_port,
                    message_received_from,
                    push_sockets_dict[message_received_from],
                )

        #barrier.wait()
        # Phase2 --------------------------------------------------

        if is_proposer:
            # As a proposer
            time.sleep(0.3)

            if will_propose:
                # Broadcast 'ACCEPT REQUEST'
                broadcastRegular(
                    f"ACCEPT REQUEST {proposeVal}",
                    my_port,
                    push_sockets_dict,
                    prob,
                )
            else:
                # Broadcast 'ROUNDCHANGE'
                print(f"{my_port} CHANGED ROUND")
                broadcastRegular(
                    "ROUNDCHANGE",
                    my_port,
                    push_sockets_dict,
                    0,
                )
                # Go for another round

        # Receive 'ACCEPT REQUEST|CRASH|ROUNDCHANGE' from proposer
        message_received = socket_pull.recv_json()

        # Parse message received
        message_received_body = message_received["body"]
        message_received_from = message_received["from"]
        message_received_to = message_received["to"]

        if is_proposer:
            # As a proposer
            if will_propose:
                # If proposer has proposed new value, then it will listen N responses
                accept_count = 0
                is_received_propose = False

                if "ROUNDCHANGE" not in message_received_body:
                    print(
                        f"PROPOSER OF {my_port} RECEIVED IN ACCEPT PHASE: {message_received_body}"
                    )

                    if "ACCEPT REQUEST" in message_received_body:
                        accept_count += 1
                        is_received_propose = True

                    elif "CRASH" in message_received_body:
                        # TODO
                        pass

                    # Receive responses from N-1 acceptors ('PROMISE|CRASH')

                    for _ in range(len(known_nodes) - 1):
                        message_received = socket_pull.recv_json()

                        # Parse message received
                        message_received_body = message_received["body"]
                        message_received_from = message_received["from"]
                        message_received_to = message_received["to"]

                        print(
                            f"PROPOSER OF {my_port} RECEIVED IN ACCEPT PHASE: {message_received_body}"
                        )

                        if "ACCEPT" in message_received_body:
                            accept_count += 1

                    if is_received_propose:
                        maxVotedVal = proposeVal

                    if accept_count > int(len(known_nodes) / 2):
                        decision = proposeVal
                        print(f"PROPOSER OF {my_port} DECIDED ON VALUE: {decision}")

            pass

        elif not is_proposer:
            # As an acceptor
            time.sleep(0.3)
            print(f"ACCEPTOR {my_port} RECEIVED IN ACCEPT PHASE: {message_received_body}")

            if "ACCEPT REQUEST" in message_received_body:
                # send 'ACCEPT' as an acceptor
                sendRegular(
                    "ACCEPT",
                    my_port,
                    message_received_from,
                    push_sockets_dict[message_received_from],
                    prob,
                ) 

                maxVotedVal = int(message_received_body.split(" ")[1])

            elif "ROUNDCHANGE" in message_received_body:
                pass
            elif "CRASH" in message_received_body:
                # If an acceptor receives 'CRASH', then it responds with 'CRASH' too
                sendRegular(
                    f"CRASH {my_port}",
                    my_port,
                    message_received_from,
                    push_sockets_dict[message_received_from],
                )

            pass

        #barrier.wait()
        time.sleep(0.3)
        proposalId +=1
        # Go for next round
    pass


def main(args):
    prob = float(args[1])
    my_port = int(args[2])

    doBroadcast(known_nodes, my_port)

    if len(known_nodes) > 1:
        PaxosNode(my_port, prob)

    '''# print(f"NUM NODES: {numNodes}, CRASH PROB: {prob}, NUM ROUNDS: {numRounds}")

    

    # Create processes
    # Each process represents a paxos node
    processes = []

    for my_port in range(numNodes):
        value = random.randint(0, 8)
        process = Process(
            target=PaxosNode,
            args=(
                my_port,
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

    # Wait all paxos nodes to finish rounds
    for process in processes:
        process.promise()

    pass'''

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Invalid command line arguments!")
    else:
        main(args=sys.argv)