"""
Example to show sending message(s) to a Service Bus Topic.
"""

import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.identity import DefaultAzureCredential
from azure.identity import EnvironmentCredential # for interactive browser authentication
from azure.appconfiguration.provider import (
    load,
    SettingSelector
)

endpoint = os.environ.get("AZURE_APPCONFIG_ENDPOINT")
if endpoint is None:
    raise ValueError("AZURE_APPCONFIG_ENDPOINT environment variable is not set.")

print(f"Connecting to Azure App Configuration...{endpoint}")

# Connect to Azure App Configuration using Microsoft Entra ID.
credential = DefaultAzureCredential()
configsList = load(endpoint=endpoint, credential=credential)
connstr_sb = configsList["conn_sunsbstudent_venuea"]
print(f"Connection string is {connstr_sb}")    

os.environ["SERVICEBUS_FULLY_QUALIFIED_NAMESPACE"]='sunsbstudent.servicebus.windows.net'
os.environ["SERVICEBUS_TOPIC_NAME"]='venuea'
FULLY_QUALIFIED_NAMESPACE = os.environ["SERVICEBUS_FULLY_QUALIFIED_NAMESPACE"]
TOPIC_NAME = os.environ["SERVICEBUS_TOPIC_NAME"]


def send_single_message(sender, SESSION_ID):
    message = ServiceBusMessage("Single Message")
    message.session_id = SESSION_ID
    sender.send_messages(message)
    print("Single Send message is done.")

def send_a_list_of_messages(sender, SESSION_ID):
    for i in range(1,5):
        message = ServiceBusMessage(f"Message in list {i} for {SESSION_ID}")
        message.session_id = SESSION_ID
        sender.send_messages(message)
    print(f"{SESSION_ID} - List of {i} messages, Send message is done.")

def send_batch_message(sender,SESSION_ID):
    batch_message = sender.create_message_batch()
    # Create a batch of messages. The batch can contain multiple messages.
    # The batch can be sent to the Service Bus Topic in a single call.
    #batch_message.session_id = "Gr_C"
    #batch_message.message_id = "BatchMessageID"
    #batch_message.partition_key = "Gr_C"  
    # Set message_id and application_properties to the message.
    # These properties are optional and can be set to any value.  
    #batch_message
    for jCtr in range(1,10):
        try:
            message = ServiceBusMessage(f"Message in batch {jCtr}")
            message.session_id = SESSION_ID
            #message.partition_key = "Gr_C"
            # Set message_id and application_properties to the message.
            # These properties are optional and can be set to any value.    
            #message.message_id = f"MessageID-{jCtr}"
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
    

#credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
#credential = InteractiveBrowserCredential()
servicebus_client = ServiceBusClient.from_connection_string(
    conn_str=connstr_sb
)

def send_message(sender, SESSION_ID):
    # Create a message to send to the Service Bus Topic.
    # Send a single message to the Service Bus Topic.
    send_single_message(sender,SESSION_ID)
    print("Send message is done.")

userinput = input("Press Any key to start,  Q to exit...")
while userinput != "Q":
    
    with servicebus_client:
        sender = servicebus_client.get_topic_sender(topic_name=TOPIC_NAME)
        with sender:
            #send_single_message(sender, "Gr_A")
            send_a_list_of_messages(sender,"Gr_A")
            send_a_list_of_messages(sender,"Gr_B")  
            send_a_list_of_messages(sender,"Gr_C")
            #send_batch_message(sender,"Gr_C")
            print("All Send messages are done.")
         
    userinput = input("Press Any Key to Send Another Set of messages , or Q to exit...")
sender.close()
print("Sender is closed.")
servicebus_client.close()
print("Service Bus Client is closed.")