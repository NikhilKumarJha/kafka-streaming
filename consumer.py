from confluent_kafka import Consumer, KafkaError

def consume_fake_data(consumer, topic):
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        print('Received message: {}'.format(msg.value().decode('utf-8')))

def main():
    broker = "localhost:9092"  # Replace with your Kafka broker address
    topic = "nikhil"

    consumer_config = {
        'bootstrap.servers': broker,
        'group.id': 'fake_data_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_config)
    consume_fake_data(consumer, topic)

if __name__ == "__main__":
    main()
