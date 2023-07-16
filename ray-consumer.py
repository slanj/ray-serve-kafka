import asyncio
from ray import serve
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

@serve.deployment(num_replicas=1,
                  ray_actor_options={"num_cpus": 0.1, "num_gpus": 0},
                  health_check_period_s=5,
                  health_check_timeout_s=1)
class RayConsumer:
    def __init__(self, topic):
        self.loop = asyncio.get_running_loop()

        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers='localhost:29092',
            group_id="ray-group",
            loop=self.loop)
        self.healthy = True

        self.loop.create_task(self.consume())
        self.healthy = True
    
    async def consume(self):
        await self.consumer.start()
        print("Consuming started")
        try:
            async for msg in self.consumer:
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                    msg.key, msg.value, msg.timestamp)
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await self.consumer.stop()
            self.healthy = False
    
    # Called by Ray Serve to check Replica's health
    async def check_health(self):
        if not self.healthy:
            raise RuntimeError("Kafka Consumer is broken")


topic = "ray-topic"

deployment = RayConsumer.bind(topic)