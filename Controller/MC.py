import redis
import os
import threading
import json
import numpy as np
from collections import deque
from enum import Enum
import dotenv
from ..config import ctlEnum
from ..model import groupModel, userModel, transaction
from .IC import InputConsumer

conf = {'bootstrap.servers': "kafka:9092",
        'auto.offset.reset': 'smallest',
        'group.id': "with-ance_match"}
topic = [
    "with_ance-matchSubmit",
    "with_ance-match"
]

rd = redis.StrictRedis(host='redis', port=6379, db=0)

dotenv.load_dotenv(dotenv.find_dotenv())

class MainController:
    """ entry point of matcher, main process.
    it generate controllers and use them during in loop.
    
    InputController manage kafka inputs with socket, kafka Consumer. and using add methods to input data to Redis.
    MatchController manage matching between groups in Redis.
    OutputController manage kafka outputs with kafka Producer. and using del methods to pop data from Redis.
    """
    def __init__(self):
        self.queueList = [Queue()]
        for i in ctlEnum:
            self.queueList.append(Queue())

    def run(self):
        ic = InputConsumer(topic, conf, self.queueList)
        for enum in ctlEnum:
            globals()[enum.name] = MatchController(enum.type, enum.cap, self.queueList[enum.value])
        
        ic.start()

class MatchController(threading.Thread):
    
    def __init__(self, type, cap, q):
        super().__init__()
        self.keys = []
        self.config = {
            "type": type,
            "cap": cap,
            "queue": q
        }
        self.start()

    def run(self):
        while(True):
            if not self.config["queue"].empty():
                msg = self.config["queue"].get()
                if msg[0] is "add":
                    instance(pk=msg[1],config=self.config)

class instance(threading.Thread):
    def __init__(self, pk, config):
        super().__init__()
        self.group = groupModel(pk)
        self.queue = config["queue"]

        self._pattern = f"match:wait:{config['type']}:{config['cap']}:*"
        self._keys = rd.scan_iter(match=self._pattern, count=100)
        self.ignoreList = []

        self.start()
    
    def run(self):
        pass

    def qManage(self):
        if not self.queue.empty():
            msg = self.queue.get()
            if msg[0] == "add":
                self._keyAdd(msg[1])
            elif msg[0] == "pop":
                self._keyPop(msg[1])

    def _keyAdd(self, pk):
        key = self._pattern + str(pk)
        if not self._keys.count(key):
            self._keys.append(key)
        else:
            print(f"the group {pk} already exists.")
            
    def _keyPop(self, pk):
        key = self._pattern + str(pk)
        if self._keys.count(key):
            self._keys.pop(key)
        else:
            print(f"the group {pk} not exists.")

class CommandType(Enum):
    ADD = 1
    WITHDRAWAL = 2
    ERROR = 3

class QueueManager:
    queue_dict = {}
    MAX_QUEUE_SIZE = 1000
    
    def __init__(self, sub: [str], pub: [str]):
        if sub:
            self.sub = list()
            for topic in sub:
                temp = QueueManager.queue_dict.get(topic)
                if not temp:
                    temp = self.create_queue(topic)
                self.sub.append(QueueManager.queue_dict.get(topic))
        if pub:
            self.pub = list()        
            for topic in pub:
                temp = QueueManager.queue_dict.get(topic)
                if not temp:
                    temp = self.create_queue(topic)
                self.pub.append(QueueManager.queue_dict.get(topic))
        if sub is None and pub is None:
            raise ValueError("There are no attributes for QueueManager.")
                    
    def create_queue(self, topic: str):
        QueueManager.queue_dict[topic] = Channel(topic)
        return QueueManager.queue_dict[topic]
    
    def publish(self, msg: list):
        if not isinstance(msg, list) or len(msg) != 2:
            raise ValueError("Message should be a list of size 2.")
        
        command_type = msg[0]
        if not isinstance(command_type, CommandType):
            raise ValueError("Invalid command type.")
        
        group_pk = msg[1]
        if not isinstance(group_pk, int):
            raise ValueError("Group pk should be an integer.")
        
        for q in self.pub:
            if len(q.queue) >= self.MAX_QUEUE_SIZE:
                raise ValueError("Queue size exceeded maximum limit.")
            q.queue.append(msg)
            
    def noti(self) -> list:
        non_empty_topics = []
        for topic, q in QueueManager.queue_dict.items():
            if q.queue:
                non_empty_topics.append(topic)
        return non_empty_topics
    
    def pop(self, topic: str) -> list:
        q = QueueManager.queue_dict.get(topic)
        if q and topic in self.sub:
            if q.queue:
                return q.queue.popleft()
            else:
                raise ValueError("Specified topic queue is empty.")
        else:
            raise ValueError("Specified topic is not subscribed.")
    
    def listen(self, topic: str) -> list:
        q = QueueManager.queue_dict.get(topic)
        if q and topic in self.sub:
            if q.queue:
                return q.queue[0]
            else:
                raise ValueError("Specified topic queue is empty.")
        else:
            raise ValueError("Specified topic is not subscribed.")

class Channel:
    def __init__(self, topic: str):
        self.topic = topic
        self.queue = deque()
        
    def __str__(self):
        return self.topic

class MonitorController:
    pass

MC = MainController()