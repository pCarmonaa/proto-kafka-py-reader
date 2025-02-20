# Proto Kafka Py Reader

A simple Python script to consume and deserialize Kafka messages serialized in Protocol Buffers (protobuf) format. Ideal for development or testing, it allows you to inspect the contents of a Kafka topic using a `.proto` file.

## Features
- Consumes messages from a Kafka topic.
- Deserializes protobuf messages using a provided `.proto` file.
- Prints deserialized messages to the console for easy inspection.

## Usage
1. Create a virtual environment (optional)
   ```bash
   python -m venv proto-kafka-consumer
   source proto-kafka-consumer/bin/activate  # On Windows: proto-kafka-consumer\Scripts\activate
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
4. Update the script with your Kafka broker address, topic, event name, and path to the .proto file.
5. Run the script:
   ```bash
   python proto_kafka_consumer.py

## Why?
It's common to serialize Kafka events in protobuf format for efficient storage and communication. This script helps developers and testers quickly inspect topic contents without additional tools.
