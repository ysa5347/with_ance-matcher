import json
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "kafka:9092",
}
topic = ["with_ance-transaction"]
class outputController:

    def __init__(self):
        self.producer = Producer(conf)
        self.topic = topic

    def send(self, body):
        tr = transaction(body)
        
        