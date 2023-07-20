import asyncio
import ray
from ray import serve
from aiokafka import AIOKafkaConsumer


@serve.deployment(num_replicas=1,
                  ray_actor_options={"num_cpus": 0.1, "num_gpus": 0},
                  health_check_period_s=1,
                  health_check_timeout_s=1)
class RayConsumer:
    def __init__(self, topic, model):
        self.model = model
        self.loop = asyncio.get_running_loop()

        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers='localhost:29092',
            group_id="ray-group",
            loop=self.loop)
        self.healthy = True

        self.loop.create_task(self.consume())
        
    async def consume(self):
        await self.consumer.start()        
        print("Consuming started")
        try:
            async for msg in self.consumer:
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                    msg.key, msg.value, msg.timestamp)
                print(self.model(msg.value))
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await self.consumer.stop()
            self.healthy = False
    
    # Called by Ray Serve to check Replica's health
    async def check_health(self):
        if not self.healthy:
            raise RuntimeError("Kafka Consumer is broken")


topic = "ray-topic"

def toy_model(text):
    return "predicted: " + text.decode("utf-8")  

deployment = RayConsumer.bind(topic, toy_model)