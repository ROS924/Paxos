import socket, os, random, threading, zmq, time, numpy
from multiprocessing import Process, Barrier
#from utils import *

#---------------------------------------------------------------------------
#-------------------------CÓDIGO DO CARA------------------------------------
#---------------------------------------------------------------------------
# ver questao do crash

BASE_PORT = 5550

# Sends message with arg that permit probability
def sendRegular(body, sender_id, target_id, push_socket, prob):
    is_crashed
    if prob != 0:
        is_crashed = numpy.random.choice([True, False], p=[prob, 1 - prob])

    message = {}
    if is_crashed:
        message = {"body": f"CRASH {sender_id}", "from": sender_id, "to": target_id}
    else:
        message = {"body": body, "from": sender_id, "to": target_id}

    push_socket.send_json(message)

# Broadcast message without crash probability
def broadcastRegular(body, sender_id, numNodes, push_sockets_dict, prob, am_i_excluded=False):
    for target_id in range(numNodes):
        push_socket = push_sockets_dict[target_id]

        if am_i_excluded and target_id == sender_id:
            continue

        # send message to target
        sendRegular(body, sender_id, target_id, push_socket, prob)



def PaxosNode(node_id, value, numNodes, prob, numRounds, barrier):
    maxVotedRound = -1  # Proposer & Acceptor
    maxVotedVal = None  # Proposer & Acceptor
    proposeVal = None  # Only Proposer
    decision = None  # Only Proposer

    # Create PULL socket (1 socket) (use bind since it will receive messages from N nodes)
    context = zmq.Context()
    socket_pull = context.socket(zmq.PULL)
    socket_pull.bind(f"tcp://127.0.0.1:{BASE_PORT + node_id}")

    # Create PUSH sockets (N sockets) (use connect since they will be used to send 1 message)
    push_sockets_dict = {}

    for target_id in range(numNodes):
        socket_push = context.socket(zmq.PUSH)
        socket_push.connect(f"tcp://127.0.0.1:{BASE_PORT + target_id}")
        push_sockets_dict[target_id] = socket_push

    # Wait for everyone finishing establishing their connections
    time.sleep(0.3)

    # Run algorithm
    for r in range(numRounds):

        is_proposer = r % numNodes == node_id

        if is_proposer:
            print(f"PROPOSER OF ROUND {r} PROPOSED VALUE: {value}")
            # Broadcast 'READY'

            time.sleep(0.3)

            broadcastRegular(
                "READY",
                node_id,
                numNodes,
                push_sockets_dict,
                prob,
            )

        # Receive 'READY|CRASH' from proposer
        message_received = socket_pull.recv_json()

        # Parse message received
        message_received_body = message_received["body"]
        message_received_from = message_received["from"]
        message_received_to = message_received["to"]

        time.sleep(0.3)

        # Phase1 --------------------------------------------------
        join_count = 0
        will_propose = False

        if is_proposer:
            print(
                f"PROPOSER OF {node_id} RECEIVED IN PREPARE PHASE: {message_received_body}"
            )

            # As a proposer:
            is_received_start = False

            if "READY" in message_received_body:
                join_count += 1
                is_received_start = True

            # Receive responses from N-1 acceptors ('JOIN|CRASH')

            received_maxVotedRound = -1
            received_maxVotedVal = -1

            for _ in range(numNodes - 1):
                message_received = socket_pull.recv_json()

                # Parse message received
                message_received_body = message_received["body"]
                message_received_from = message_received["from"]
                message_received_to = message_received["to"]

                print(
                    f"PROPOSER OF {node_id} RECEIVED IN PREPARE PHASE: {message_received_body}"
                )

                if "JOIN" in message_received_body:
                    join_count += 1

                    # Incoming message "JOIN {maxVotedRound} {maxVotedVal}"
                    parsed_join = message_received_body.split(" ")

                    if int(parsed_join[1]) > received_maxVotedRound:
                        # If incoming JOIN's maxVotedVal is bigger than previous
                        # Then update previous with incoming maxVotedVal, round too
                        # This is basically for picking the JOIN message with biggest maxVotedRound
                        # Then we can set proposeVal to this message's maxVotedVal
                        received_maxVotedRound = int(parsed_join[1])
                        received_maxVotedVal = int(parsed_join[2])

            # If majority joined
            if join_count > int(numNodes / 2):
                will_propose = True

                # If proposer received 'READY' from itself in the beginning
                # And if maxVotedRound is -1, then update proposeVal
                if is_received_start:
                    if maxVotedRound == -1:
                        proposeVal = value
                    else:
                        # SUSPICIOUS
                        proposeVal = received_maxVotedVal

                # If proposer didn't receive 'READY' from itself in the beginning,
                # Then set maxVotedRound to the maximum maxVotedRound came from JOINs
                # And set maxVotedVal to the maxixmum maxVotedVal came from JOINs
                else:
                    proposeVal = value

            # If majority didn't join
            else:
                will_propose = False

        elif not is_proposer:
            # As an acceptor:
            print(f"ACCEPTOR {node_id} RECEIVED IN PREPARE PHASE: {message_received_body}")

            if "READY" in message_received_body:

                time.sleep(0.3)

                # Send "JOIN" to proposer
                sendRegular(
                   f"JOIN {maxVotedRound} {maxVotedVal}",
                    node_id,
                    message_received_from,
                    push_sockets_dict[message_received_from],
                    prob,
                )

            elif "CRASH" in message_received_body:
                # If an acceptor receives 'CRASH', then it responds with 'CRASH' too
                sendRegular(f"CRASH {node_id}",
                    node_id,
                    message_received_from,
                    push_sockets_dict[message_received_from],
                )

        barrier.wait()
        # Phase2 --------------------------------------------------

        if is_proposer:
            # As a proposer
            time.sleep(0.3)

            if will_propose:
                # Broadcast 'PROPOSE'
                broadcastRegular(
                    f"PROPOSE {proposeVal}",
                    node_id,
                    numNodes,
                    push_sockets_dict,
                    prob,
                )
            else:
                # Broadcast 'ROUNDCHANGE'
                print(f"PROPOSER OF ROUND {r} CHANGED ROUND")
                broadcastRegular(
                    "ROUNDCHANGE",
                    node_id,
                    numNodes,
                    push_sockets_dict,
                    0,
                )
                # Go for another round

        # Receive 'PROPOSE|CRASH|ROUNDCHANGE' from proposer
        message_received = socket_pull.recv_json()

        # Parse message received
        message_received_body = message_received["body"]
        message_received_from = message_received["from"]
        message_received_to = message_received["to"]

        if is_proposer:
            # As a proposer
            if will_propose:
                # If proposer has proposed new value, then it will listen N responses
                vote_count = 0
                is_received_propose = False

                if "ROUNDCHANGE" not in message_received_body:
                    print(
                        f"PROPOSER OF {node_id} RECEIVED IN ACCEPT PHASE: {message_received_body}"
                    )

                    if "PROPOSE" in message_received_body:
                        vote_count += 1
                        is_received_propose = True

                    elif "CRASH" in message_received_body:
                        # TODO
                        pass

                    # Receive responses from N-1 acceptors ('JOIN|CRASH')

                    for _ in range(numNodes - 1):
                        message_received = socket_pull.recv_json()

                        # Parse message received
                        message_received_body = message_received["body"]
                        message_received_from = message_received["from"]
                        message_received_to = message_received["to"]

                        print(
                            f"PROPOSER OF {node_id} RECEIVED IN ACCEPT PHASE: {message_received_body}"
                        )

                        if "VOTE" in message_received_body:
                            vote_count += 1

                    if is_received_propose:
                        maxVotedRound = r
                        maxVotedVal = proposeVal

                    if vote_count > int(numNodes / 2):
                        decision = proposeVal
                        print(f"PROPOSER OF {node_id} DECIDED ON VALUE: {decision}")

            pass

        elif not is_proposer:
            # As an acceptor
            time.sleep(0.3)
            print(f"ACCEPTOR {node_id} RECEIVED IN ACCEPT PHASE: {message_received_body}")

            if "PROPOSE" in message_received_body:
                # send 'VOTE' as an acceptor
                sendRegular(
                    "VOTE",
                    node_id,
                    message_received_from,
                    push_sockets_dict[message_received_from],
                    prob,
                )
                maxVotedRound = r
                maxVotedVal = int(message_received_body.split(" ")[1])

            elif "ROUNDCHANGE" in message_received_body:
                pass
            elif "CRASH" in message_received_body:
                # If an acceptor receives 'CRASH', then it responds with 'CRASH' too
                sendRegular(
                    f"CRASH {node_id}",
                    node_id,
                    message_received_from,
                    push_sockets_dict[message_received_from],
                )

            pass

        barrier.wait()
        time.sleep(0.3)
        # Go for next round
    pass


def main(args):
    print("RODOU")
    numNodes = int(args[1])
    prob = float(args[2])
    numRounds = int(args[3])

    barrier = Barrier(numNodes)

    print(f"NUM NODES: {numNodes}, CRASH PROB: {prob}, NUM ROUNDS: {numRounds}")

    # Create processes
    # Each process represents a paxos node
    processes = []

    for node_id in range(numNodes):
        value = random.randint(0, 1)
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

    # Wait all paxos nodes to finish rounds
    for process in processes:
        process.join()

    pass

main()