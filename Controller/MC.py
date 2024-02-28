import redis
import os
from queue import Queue
import threading
import json
import numpy as np
import dotenv
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
        self.queue = Queue()

    def run(self):
        ic = InputConsumer(topic, conf, self.queue)
        mc = MatchController(self.queue)
        
        ic.start()
        mc.start()


class MatchController(threading.Thread):
    
    def __init__(self, q):
        self.q = q
        self.wait = []

        

    def run(self):
        


class instance(threading.Thread):
    pass

class MonitorController:
    pass