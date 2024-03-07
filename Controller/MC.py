import redis
import os
from queue import Queue
import threading
import json
import numpy as np
import dotenv
from config import ctlEnum
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

    def _keyAdd(self, pk):
        key = self._pattern + str(pk)
        if not self._keys.count(key):
            self._keys.append(key)
        else:
            raise ValueError(f"the group {pk} already exist.")
            
    def _keyPop(self, pk):
        pass
        
class MonitorController:
    pass

MC = MainController()