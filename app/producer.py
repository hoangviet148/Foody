import pika
import warnings
import json
import time 
warnings.filterwarnings("ignore")

credentials = pika.PlainCredentials('hoang', 'hoang')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq', credentials=credentials))

channel = connection.channel()

channel.exchange_declare(exchange='amq.topic', exchange_type='topic', durable=True)

data = "ngo viet hoang tran tan minh"
for i in range(5):
    channel.basic_publish(
        exchange='amq.topic',  # amq.topic as exchange
        routing_key='foody',   # Routing key used by producer
        body='Hello World {0}'.format(i)
    )
    time.sleep(3)
connection.close()