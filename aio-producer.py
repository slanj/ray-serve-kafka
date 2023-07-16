from aiokafka import AIOKafkaProducer
import asyncio
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--topic', nargs='?', const='ray-topic', type=str, default='ray-topic')
parser.add_argument('--message', nargs='?', const='Consuming from Kafka with Ray!', type=str, default='Consuming from Kafka with Ray!')
parser.add_argument('--kafka', nargs='?', const='localhost:29092', type=str, default='localhost:29092')
args = parser.parse_args()
print(args)

async def send_one():
    producer = AIOKafkaProducer(bootstrap_servers=args.kafka)
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait(args.topic, str.encode(args.message))
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()

asyncio.run(send_one())