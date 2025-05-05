
"""
 Session Enabled receiving  messages from a Service Bus Subscription under specific Topic.
"""

import os
from azure.servicebus import ServiceBusClient
from azure.identity import DefaultAzureCredential
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
SUBSCRIPTION_NAME_G1 = 'Group1'
SUBSCRIPTION_NAME_G2 = 'Group2'
SUBSCRIPTION_NAME_G3 = 'Group3'
Recevier_Session_ID_A = 'Gr_A' # for Group 1
Recevier_Session_ID_B = 'Gr_B' # for Group 2    

Recevier_Session_ID_C = 'Gr_C' # for Group 3
#Recevier_Session_ID = 'Gr_C_1' # for Group 3
servicebus_client = ServiceBusClient.from_connection_string(
    conn_str=connstr_sb
)


# Receive a set of messages from a subscription under a topic.
def receive_messages(SUBSCRIPTION_NAME, Rxr_session_id):
    print(f'Trying to get messages. for {SUBSCRIPTION_NAME} - {Rxr_session_id}')
    with servicebus_client:
        receiver = servicebus_client.get_subscription_receiver(topic_name=TOPIC_NAME, 
                                                            subscription_name=SUBSCRIPTION_NAME,
                                                            session_id=Rxr_session_id)
        # receiver.session_id = "Gr_A" #Setting it this way will not work.Set it as above on the constructor.
        with receiver:
            received_msgs = receiver.receive_messages(max_message_count=50, max_wait_time=5)
            print(f"Received {len(received_msgs)} messages. for {SUBSCRIPTION_NAME} - {Rxr_session_id}")
            for msg in received_msgs:
                print(str(msg))
                
                receiver.complete_message(msg)
            receiver.close()
userinput = input("Press Any key to start,  Q to exit...")
while userinput != "Q":
    
    receive_messages(SUBSCRIPTION_NAME_G1, Recevier_Session_ID_A)
    receive_messages(SUBSCRIPTION_NAME_G2, Recevier_Session_ID_B)
    #receive_messages(SUBSCRIPTION_NAME_G3, Recevier_Session_ID_C)   
    
    print("All receive messages are done.")
            
    userinput = input("Press Any Key to Receive Another Set of messages , or Q to exit...")
servicebus_client.close()
print("Service Bus Client is closed.")  
