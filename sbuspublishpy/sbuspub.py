"""
sending message(s) to a Service Bus Topic.
"""

import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage, TransportType
from azure_appconfig import get_servicebus_connection_string

# Get the Service Bus connection string from Azure App Configuration
connstr_sb = get_servicebus_connection_string()

os.environ["SERVICEBUS_FULLY_QUALIFIED_NAMESPACE"] = 'sunsbstudent.servicebus.windows.net'
os.environ["SERVICEBUS_TOPIC_NAME"] = 'venueb'
FULLY_QUALIFIED_NAMESPACE = os.environ["SERVICEBUS_FULLY_QUALIFIED_NAMESPACE"]
TOPIC_NAME = os.environ["SERVICEBUS_TOPIC_NAME"]
globalCounter = 0

def send_single_message(sender, SESSION_ID):
    message = ServiceBusMessage("Single Message")
    message.session_id = SESSION_ID
    sender.send_messages(message)
    print("Single Send message is done.")

def send_a_list_of_messages(sender, SESSION_ID):
    for i in range(1, 2):
        global globalCounter
        globalCounter += 1
        # Create a message to send to the Service Bus Topic.
        message = ServiceBusMessage(f"Global Counter {globalCounter}- Message in list {i} for {SESSION_ID}")
        message.session_id = SESSION_ID  # LBN user name
        # set message_id and application_properties to the message.
        # These properties are optional and can be set to any value.
        message.application_properties = {"incomingorder": f"value-{i}", "name": f"sundartest"}
        # message.message_id = f"MessageID-{i}"

        sender.send_messages(message)
    print(f"{SESSION_ID} - List of {i} messages, Send message is done.")

def send_batch_message(sender, SESSION_ID):
    batch_message = sender.create_message_batch()
    # Create a batch of messages. The batch can contain multiple messages.
    # The batch can be sent to the Service Bus Topic in a single call.
    # batch_message.session_id = "Gr_C"
    # batch_message.message_id = "BatchMessageID"
    # batch_message.partition_key = "Gr_C"
    # Set message_id and application_properties to the message.
    # These properties are optional and can be set to any value.
    # batch_message
    for jCtr in range(1, 10):
        try:
            message = ServiceBusMessage(f"Message in batch {jCtr}")
            message.session_id = SESSION_ID
            # message.partition_key = "Gr_C"
            # Set message_id and application_properties to the message.
            # These properties are optional and can be set to any value.
            # message.message_id = f"MessageID-{jCtr}"
            message.application_properties = {"key": f"value-{jCtr}"}
            # Add message to batch. If the batch is full, it will raise a ValueError.
            batch_message.add_message(message)
            print(f"Batch message {jCtr} is added.")
        except ValueError:
            # ServiceBusMessageBatch object reaches max_size.
            # New ServiceBusMessageBatch object can be created here to send more data.
            break
        try:
            sender.send_messages(batch_message)
            print("Batch Send message is done.")
        except Exception as e:
            # ServiceBusMessageBatch object reaches max_size.
            # New ServiceBusMessageBatch object can be created here to send more data.
            print(f"Batch message failed. {e}")
            break

# Send a message to the Service Bus Topic.
# The message can be sent to the Service Bus Topic in a single call.
def send_message(sender, SESSION_ID):
    # Create a message to send to the Service Bus Topic.
    # Send a single message to the Service Bus Topic.
    send_single_message(sender, SESSION_ID)
    print("Send message is done.")

# credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
# credential = InteractiveBrowserCredential()
servicebus_client = ServiceBusClient.from_connection_string(
    conn_str=connstr_sb, transport_type=TransportType.Amqp
)
print(f"Service Bus Client is created. {FULLY_QUALIFIED_NAMESPACE} - {TOPIC_NAME}")
#print(f'servicebus_client: {servicebus_client}')
with servicebus_client:
    sender = servicebus_client.get_topic_sender(topic_name=TOPIC_NAME)
    
    userinput = input("Press Any key to start,  Q to exit...")
    while userinput != "Q":
        
                #send_single_message(sender, "Gr_1")
                send_a_list_of_messages(sender, "Gr_1")
                send_a_list_of_messages(sender, "Gr_2")  
                send_a_list_of_messages(sender, "Gr_3")
                #send_batch_message(sender,"Gr_3")
                print("All Send messages are done.")            
                userinput = input("Press Any Key to Send Another Set of messages , or Q to exit...")
sender.close()
print("Sender is closed.")
servicebus_client.close()
print("Service Bus Client is closed.")