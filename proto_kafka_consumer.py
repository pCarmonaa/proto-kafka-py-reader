import os
import sys
import json
from confluent_kafka import Consumer, KafkaError
import grpc
from google.protobuf import message
from google.protobuf import json_format

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'set-your-topic-here'
PROTO_FILE = './path/to/proto/file'
PROTO_MESSAGE_NAME = 'set-message-name-here'

GROUP_ID = 'python_consumer'
AUTO_OFFSET_RESET = 'latest'
ENABLE_AUTO_COMMIT = False

# NOTE: Before running this script, ensure the following:
#
# 1. The file with the event definitions (e.g., eventdefinitions.proto) must be located in the 'protos' directory.
# 2. Any external .proto libraries (e.g., google/protobuf/timestamp.proto, google/protobuf/wrappers.proto)
#    required by your .proto definitions must be downloaded manually and placed in the appropriate folder
#    inside the 'protos' directory (https://github.com/protocolbuffers/protobuf/tree/main/src/google/protobuf). For example:
#
#    /my_project
#       /protos
#           /google
#               /protobuf
#                   timestamp.proto
#                   wrappers.proto
#           eventdefinitions.proto
#
# Once all necessary .proto files are in the correct locations, the script can run without additional compilation steps.
#
# Ensure the generated Python files (eventdefinitions_pb2.py, etc.) are correctly imported and used within the script.

def generate_protobuf_code(proto_file):
    from grpc_tools import protoc
    protoc.main((
        '',
        f'--proto_path={os.path.dirname(proto_file)}',
        f'--python_out=.',
        f'--grpc_python_out=.',
        proto_file,
    ))

def import_generated_protobuf_module(proto_file, message_name):
    proto_filename = os.path.splitext(os.path.basename(proto_file))[0]
    generated_module_name = f'{proto_filename}_pb2'
    
    generated_module = __import__(generated_module_name)    
    message_class = getattr(generated_module, message_name)
    
    return message_class

def create_kafka_consumer(broker, topic):
    consumer = Consumer({
        'bootstrap.servers': broker,
        'group.id': GROUP_ID,
        'auto.offset.reset': AUTO_OFFSET_RESET,
        'enable.auto.commit': ENABLE_AUTO_COMMIT
    })
    
    consumer.subscribe([topic])
    return consumer

def consume_and_deserialize_messages(consumer, message_class):
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    break
            
            try:
                deserialized_message = message_class()
                deserialized_message.ParseFromString(msg.value())
                
                json_message = json_format.MessageToJson(
                    deserialized_message,
                    including_default_value_fields=True,
                    preserving_proto_field_name=True,
                    use_integers_for_enums=False
                )
                
                output = {
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'message': json.loads(json_message)
                }

                print(json.dumps(output, indent=2))
            except message.DecodeError as e:
                print(f"Error deserializing message: {e}")
    
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    generate_protobuf_code(PROTO_FILE)
    message_class = import_generated_protobuf_module(PROTO_FILE, PROTO_MESSAGE_NAME)    
    kafka_consumer = create_kafka_consumer(KAFKA_BROKER, TOPIC)
    
    consume_and_deserialize_messages(kafka_consumer, message_class)