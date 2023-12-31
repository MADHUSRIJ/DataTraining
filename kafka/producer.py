from kafka import KafkaProducer
from time import sleep
from json import dumps

class Producer:
    def __init__(self, topic, server) -> None:
        self.topic = topic
        self.server = server
        self.producer = KafkaProducer(bootstrap_servers=[self.server])

    def send(self, data):
        self.producer.send(self.topic, dumps(data).encode("utf-8"))
        sleep(1)

producer = Producer("New_Topic", "localhost:9092")
data = ["M","A","D","H","U","S","R","I"]

for d in data:
    producer.send(d)