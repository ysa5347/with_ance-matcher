import json
import sys 
import threading
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
from queue import Queue
from ..config import ctlEnum
from ..model import groupModel, userModel
import redis


#We want to run thread in an infinite loop
running = True

class InputConsumer(threading.Thread):
    def __init__(self, topic, conf, queueList):
        threading.Thread.__init__(self)
        # Create consumer
        self.consumer = Consumer(conf)
        self.topic = topic
        self.queueList = queueList
   
    def run(self):
        print ('Inside MatchService :  Created Listener ')
        try:
            self.consumer.subscribe([self.topic])
            while running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None: continue
                #Handle Error
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event; 10초간 poll하지 않을 경우 발생.
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                    else:
                        raise KafkaException(msg.error())
                else:
                    #Handle Message
                    print('---------> Got message, push to redis.....')
                    message = json.loads(msg.value().decode('utf-8'))
                    match message["status"]:
                        case "error":
                            print(message["body"])
                        case "submit":
                            self.submit(msg)
                        case "withdrawal":
                            self.withdrawal(msg)

        finally:
        # Close down consumer to commit final offsets.
            self.consumer.close()
    
    def submit(self, message):
        """submit methods;
            add models, put create message to queue."""
        msg = ["add", message["group"]["pk"]]
        q = self.queueList[ctlEnum.getValue(message["type"], message["group"]["userCap"])]
        q.put(msg)

        model = groupModel(message)


    def withdrawal(self, message):
    
        msg = ["pop", message["group"]["pk"]]
        q = self.queueList[ctlEnum.getValue(message["type"], message["group"]["userCap"])]
        q.put(msg)

        model = groupModel(message)