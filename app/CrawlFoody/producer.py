import pika
import time
import json
from crawler import get_data
import warnings
import glob
warnings.filterwarnings("ignore")

USER_NAME = "hoang"
PASSWORD = "hoang"
HOST_ADDR = "192.168.1.129"
BATCH_SIZE = 1

credentials = pika.PlainCredentials(USER_NAME, PASSWORD)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=HOST_ADDR, credentials=credentials))


channel = connection.channel()

channel.exchange_declare(exchange='amq.topic', exchange_type='topic', durable=True)

folder_data = "./data/split_6/"
list_path_data = glob.glob(folder_data + "*.csv")
for path_data in list_path_data:
    data = get_data(path_data)

    data = json.loads(data)

    for d in data:
        channel.basic_publish(exchange='amq.topic', routing_key='foody', body=json.dumps(d))
        
    time.sleep(3)


connection.close()




