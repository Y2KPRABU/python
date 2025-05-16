#Here’s how you can post a message to a Kafka topic using Python. The most common library for interacting with Kafka in Python is confluent-kafka or kafka-python. Below are examples using both libraries:

"""#1. Using confluent-kafka
from confluent_kafka import Producer

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
}

# Create a producer instance
producer = Producer(conf)

# Callback to confirm message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Produce a message
topic = "my_topic"
message = "Hello, Kafka!"

producer.produce(topic, value=message, callback=delivery_report)

# Wait for all messages to be delivered
producer.flush()"""

#2. Using kafka-python
from kafka import KafkaProducer

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Replace with your Kafka broker(s)
    value_serializer=lambda v: v.encode('utf-8')  # Serialize message to bytes
    ,client_id="sundar_client_1", key_serializer=lambda v: v.encode('utf-8')
)

TOPIC_NAME = "Topic_VenA"
print(f"Kafka Client is created. - {TOPIC_NAME}")


globalCounter = 0
# Send List of messages to Kafka topic
def send_a_list_of_messages( SESSION_ID):
    for i in range(1, 2):
        global globalCounter
        globalCounter += 1
        # Create a message to send to the Service Bus Topic.
        message = f"Global Counter {globalCounter}- Message in list {i} for {SESSION_ID}"
        # set message_id and application_properties to the message.
        # These properties are optional and can be set to any value.
        #message.application_properties = {"incomingorder": f"value-{i}", "name": f"sundartest"}
        # message.message_id = f"MessageID-{i}"

        producer.send(TOPIC_NAME, value=message,key=SESSION_ID)
    print(f"{SESSION_ID} - List of {i} messages, Send message is done.")


userinput = input("Press Any key to start,  Q to exit...")
    
while userinput.capitalize() != "Q":
        
                #send_single_message(sender, "Gr_1")
                send_a_list_of_messages( "Gr_1")
                send_a_list_of_messages( "Gr_2")  
                send_a_list_of_messages( "Gr_3")
                #send_batch_message(sender,"Gr_3")
                print("All Send messages are done.")            
                userinput = input("Press Any Key to Send Another Set of messages , or Q to exit...")
  # Specify partition if needed
producer.flush()  # Ensure all messages are sent
producer.close()  # Close the producer
print("Sender is closed.")
print("Service Bus Client is closed.")


"""3. Using aiokafka (Asynchronous)
import asyncio
from aiokafka import AIOKafkaProducer

async def send_message():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092'  # Replace with your Kafka broker(s)
    )
    await producer.start()
    try:
        topic = "my_topic"
        message = "Hello, Kafka!"
        await producer.send_and_wait(topic, message.encode('utf-8'))
        print(f"Message sent to topic '{topic}'")
    finally:
        await producer.stop()

# Run the async function
asyncio.run(send_message())"""
