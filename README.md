# ray-serve-kafka
Integrate Distributed Ray Serve Deployment with Kafka 

### Install dependencies:
pip install -r requirements.txt

### Run Serve deployment:
serve run ray-consumer:deployment

### Send test message to Kafka
python3 aio-producer.py --message "From Kafka to Ray" --topic "ray-topic"
