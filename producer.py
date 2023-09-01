from faker import Faker
from confluent_kafka import Producer
from datetime import datetime
import random
import time

fake=Faker()

status_codes=[100,101,102,200,201,202,204,206,300,301,302,303,304,307,308,400,401,403,404,405,406,407,408,409,410,411,412,413,414,415,416,417,418,421,422,423,424,426,428,429,431,451,
500,501,502,503,504,505,507,508,510,511]

file_extensions = [".html", ".css", ".js", ".jpg", ".png", ".pdf", ".txt", ".doc", ".mp3", ".mp4"]

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def produce_fake_data(producer,topic):
    id=1
    num=10
    while 1:
        fake_time=datetime.now().strftime("%H:%M:%S")
        random_status_code=status_codes[random.randint(1,len(status_codes)-1)%len(status_codes)]
        folders = ["".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=random.randint(1, 10))) for _ in range(random.randint(1, 2))]
        url_path = "/" + "/".join(folders) + "/" + "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=random.randint(1, 10))) + random.choice(file_extensions)
        fake_data=f"{fake_time} 127.0.0.1 GET {url_path} {random_status_code}"
        producer.produce(topic,key=None,value=str(fake_data),callback=delivery_report)
        producer.flush()
        id+=1
        num+=1
        time.sleep(2)

def main():
    broker = "localhost:9092" 
    topic = "nikhil"
    num_messages = 10

    producer_config={
        'bootstrap.servers':broker
    }
    producer=Producer(producer_config)
    produce_fake_data(producer,topic)

if __name__=="__main__":
    main()