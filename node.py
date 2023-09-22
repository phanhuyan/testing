import socket
import time
import threading
import json
import sys
import random
import datetime
import logging
import argparse
HOST_NAME_LIST = [
    'fa23-cs425-8001.cs.illinois.edu',
    'fa23-cs425-8002.cs.illinois.edu',
    'fa23-cs425-8003.cs.illinois.edu',
    'fa23-cs425-8004.cs.illinois.edu',
    'fa23-cs425-8005.cs.illinois.edu',
    'fa23-cs425-8006.cs.illinois.edu',
    'fa23-cs425-8007.cs.illinois.edu',
    'fa23-cs425-8008.cs.illinois.edu',
    'fa23-cs425-8009.cs.illinois.edu',
    'fa23-cs425-8010.cs.illinois.edu',
]
Introducor = 'fa23-cs425-8002.cs.illinois.edu'
DEFAULT_PORT_NUM = 12346

logging.basicConfig(level=logging.DEBUG,
                    filename='output.log',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Server:
    def __init__(self,args):
        self.ip = socket.gethostname()
        self.port = DEFAULT_PORT_NUM
        self.heartbeat = 0
        self.timejoin = int(time.time())
        self.id = f"{self.ip}:{self.port}:{self.timejoin}"
        self.addr = (self.ip, self.port)
        self.MembershipList = {
            f"{ip}:{port}:{self.timejoin}": {
                "id": f"{ip}:{port}:{self.timejoin}",
                "addr": (ip, port),
                "heartbeat": 0,
                "status": "Alive",
                "time": time.time(), 
            }
            for ip, port in [(IP, DEFAULT_PORT_NUM) for IP in [self.ip, Introducor]]
        }
        self.failMemberList = {}
        self.failure_time_threshold = 5
        self.cleanup_time_threshold = 10
        self.suspect_time_threshold = 5
        self.n_send = 2
        self.protocol_period = args.protocol_period
        self.status = "Alive"
        self.drop_rate = args.drop_rate
        self.rlock = threading.RLock()
        self.enable_sending = True
        self.gossipS = False
        self.falsePositive = 0
        self.machineCheck = 0
    def checkFalsePositive(self):
        all_member = [f"{ip}:{port}" for ip, port in [(IP, DEFAULT_PORT_NUM) for IP in HOST_NAME_LIST]]
        for member in all_member:
            if member not in self.MembershipList:
                self.falsePositive += 1
        self.machineCheck += 10
        print("False Positive: ", self.falsePositive, "Machine check:", self.machineCheck)


    
    def printID(self):
        with self.rlock:
            print(self.id)

    def updateMembershipList(self, membershipList):
        with self.rlock:
            # only update member with increase heartbeat
            for member_id, member_info in membershipList.items():
                # Check if the member is already in the MembershipList
                if member_id in self.failMemberList:
                    continue
                if member_id in self.MembershipList:
                    current_heartbeat = self.MembershipList[member_id]["heartbeat"]
                    # Update only if the received heartbeat is greater
                    if member_info["heartbeat"] > current_heartbeat:
                        self.MembershipList[member_id] = member_info
                        self.MembershipList[member_id]["time"] = time.time()
                else:
                    # If the member is not in the MembershipList, add it
                    self.MembershipList[member_id] = member_info
                    self.MembershipList[member_id]["time"] = time.time()
                    logger.info("[JOIN]   - {}".format(member_id))

    def detectSuspectAndFailMember(self):
        with self.rlock:
            now = int(time.time())
            # Calculate the threshold time
            failure_threshold_time = now - self.failure_time_threshold
            suspect_threshold_time = now - self.suspect_time_threshold
            # Collect members to remove
            suspect_members_detected = [member_id for member_id, member_info in self.MembershipList.items() if member_info['time'] < failure_threshold_time and member_info["status"] != "Suspect"]
            for member_id in suspect_members_detected:
                self.MembershipList[member_id]["status"] = "Suspect"
                self.MembershipList[member_id]["time"] = now
                logger.info("[SUS]    - {}".format(member_id))
                log_message = f"ID: {member_id}, Status: {self.MembershipList[member_id]['status']}, Time: {self.MembershipList[member_id]['time']}\n"
                print(log_message)
            fail_members_detected = [member_id for member_id, member_info in self.MembershipList.items() if member_info['time'] < suspect_threshold_time and member_id not in self.failMemberList and member_info['status'] == "Suspect"]
            for member_id in fail_members_detected:
                self.failMemberList[member_id] = now
                del self.MembershipList[member_id]
                logger.info("[DELETE] - {}".format(member_id))

    def detectFailMember(self):
        with self.rlock:
            now = int(time.time())
            # Calculate the threshold time
            threshold_time = now - self.failure_time_threshold
            # Collect members to remove
            fail_members_detected = [member_id for member_id, member_info in self.MembershipList.items() if member_info['time'] < threshold_time and member_id not in self.failMemberList]
            for member_id in fail_members_detected:
                self.failMemberList[member_id] = now
                del self.MembershipList[member_id]
                logger.info("[DELETE] - {}".format(member_id))

    def removeFailMember(self):
            # Remove the members from the failMembershipList
        with self.rlock:
            now = int(time.time())
            threshold_time = now - self.cleanup_time_threshold
            fail_members_to_remove = [member_id for member_id, fail_time in self.failMemberList.items() if fail_time < threshold_time]
            for member_id in fail_members_to_remove:
                del self.failMemberList[member_id]

    def json(self):
        with self.rlock:
            return {
                m['id']:{
                    'id': m['id'],
                    'addr': m['addr'],
                    'heartbeat': m['heartbeat'],
                    'status': m['status']
                }
                for m in self.MembershipList.values()
            }

    def printMembershipList(self):
        with self.rlock:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_message = f"{timestamp} ==============================================================\n"
            log_message += f" {str(self.failMemberList)}\n"
            for member_id, member_info in self.MembershipList.items():
                log_message += f"ID: {member_info['id']}, Heartbeat: {member_info['heartbeat']}, Status: {member_info['status']}, Time: {member_info['time']}\n"
            print(log_message)
            print("GOSSIP+S: {}".format(self.gossipS))

    def chooseMemberToSend(self):
        with self.rlock:
            candidates = list(self.MembershipList.keys())
            random.shuffle(candidates)  # Shuffle the list in-place
            return candidates[:self.n_send]
    
    def receive(self):
        """
        A server's receiver is respnsible to receive all gossip UDP message:
        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(self.addr)
            while True:
                try:
                    # UDP receiver
                    data, server = s.recvfrom(4096)
                    # * if receives data
                    if data:
                        if random.random() < self.drop_rate:
                            continue
                        else:
                            msgs = json.loads(data.decode('utf-8'))
                            # * print out the info of the received message
                            self.updateMembershipList(msgs) 
                except Exception as e:
                    print(e)
    def user_input(self):
        """
        Toggle the sending process on or off.
        :param enable_sending: True to enable sending, False to disable sending.
        """
        while True:
            user_input = input("Enter 'join' to start sending, 'leave' to leave the group, 'enable gossip' for GOSSIP+S mode and 'disable gossip' for GOSSIP mode, 'enable suspicion' to enable suspect and 'disable suspicion' to disable suspect.")
            if user_input == 'join':
                self.enable_sending = True
                print("Starting to send messages.")
                self.MembershipList = {
                f"{ip}:{port}:{self.timejoin}": {
                "id": f"{ip}:{port}:{self.timejoin}",
                "addr": (ip, port),
                # "timejoin": self.timejoin,  # Initialize with self.timejoin
                "heartbeat": 0,
                "status": "Alive",
                "time": time.time(),  # Initialize with self.timejoin
                }
                for ip, port in [(IP, DEFAULT_PORT_NUM) for IP in [self.ip, Introducor]]
            }    
            elif user_input == 'leave':
                self.enable_sending = False
                print("Leaving the group.")
            elif user_input == 'enable suspicion':
                self.gossipS = True
                print("Starting gossip S.")
            elif user_input == 'disable suspicion':
                self.gossipS = False
                print("Stopping gossip S.")
            elif user_input == 'list_mem':
                self.printMembershipList()
            elif user_input == 'list_self':
                self.printID()
            else:
                print("Invalid input. Please enter 'join' or 'leave' to start/leave the group, 'enable suspicion' to enable suspect and 'disable suspicion' to disable suspect\
                      or 'list_mem' to print membership list, 'list_self' to print ID.")

    def send(self):
        """
        A UDP sender for a node. It sends json message to random N nodes periodically
        and maintain time table for handling timeout issue.
        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            while True:
                try:
                    if self.enable_sending:  # Check if sending is enabled
                        self.update_heartbeat()
                        peers = self.chooseMemberToSend()
                        for peer in peers:
                            send_msg = self.json()
                            s.sendto(json.dumps(send_msg).encode('utf-8'), tuple(self.MembershipList[peer]['addr']))
                    time.sleep(self.protocol_period)          
                except Exception as e:
                    print(e)
                    
    def update_heartbeat(self):
        with self.rlock:
            self.heartbeat += 1
            self.MembershipList[self.id]["status"] = "Alive"
            self.MembershipList[self.id]["heartbeat"] = self.heartbeat
            self.MembershipList[self.id]["time"] = time.time()
            if self.gossipS:
                self.detectSuspectAndFailMember()
            else:
                self.detectFailMember()
            self.removeFailMember()
            self.checkFalsePositive()

    def run(self):
        """
        Run a server as a node in group.
        There are totally two parallel processes for a single node:
        - receiver: receive all UDP message
        - sender: send gossip message periodically

        :return: None
        """
        # heartbeat_thread = threading.Thread(target=self.update_heartbeat)
        # heartbeat_thread.daemon = True
        # heartbeat_thread.start()
        
        receiver_thread = threading.Thread(target=self.receive)
        receiver_thread.daemon = True
        receiver_thread.start()

        # Start a sender thread
        sender_thread = threading.Thread(target=self.send)
        sender_thread.daemon = True
        sender_thread.start()

        # Start a to update enable sending
        user_thread = threading.Thread(target=self.user_input)
        user_thread.daemon = True
        user_thread.start()

        # You can add any additional logic or tasks here that the server should perform.

        # Wait for the threads to finish (if needed)
        # heartbeat_thread.join()
        receiver_thread.join()
        sender_thread.join()
        user_thread.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--protocol-period', type=int, help='Protocol period T in seconds', default=0.25)
    parser.add_argument('-d', '--drop-rate', type=float,
                        help='The message drop rate',
                        default=0)
    args = parser.parse_args()
    
    server = Server(args)
    server.run()
