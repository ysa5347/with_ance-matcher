import json
import sys 
import threading
from confluent_kafka import Consumer
from confluent_kafka import KafkaError
from confluent_kafka import KafkaException
import redis

rd = redis.StrictRedis(host='localhost', port=6379, db=0)
#We want to run thread in an infinite loop
running = True
conf = {'bootstrap.servers': "kafka:9092",
        'auto.offset.reset': 'smallest',
        'group.id': "with-ance_match"}
topic = [
    "with_ance-matchSubmit",
    "with_ance-match"
]
class inputConsumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        # Create consumer
        self.consumer = Consumer(conf)
        self.topic = topic
   
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
                            controller = submitController(message)
                        case "withdrawal":
                            controller = withdrawalController(message)
                    # controller.run

        finally:
        # Close down consumer to commit final offsets.
            self.consumer.close()

class submitController:
    
    def __init__(self, message):
        self.body = message

class withdrawalController:

    def __init__(self, pk):
        self.group = pk
