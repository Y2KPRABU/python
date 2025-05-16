
"""
 Session Enabled receiving  messages from a Service Bus Subscription under specific Topic.
"""

import os
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
from azure_appconfig import get_servicebus_connection_string
# Get the Service Bus connection string from Azure App Configuration
connstr_sb = get_servicebus_connection_string()

os.environ["SERVICEBUS_FULLY_QUALIFIED_NAMESPACE"]='sunsbstudent.servicebus.windows.net'
os.environ["SERVICEBUS_TOPIC_NAME"]='venuea'
FULLY_QUALIFIED_NAMESPACE = os.environ["SERVICEBUS_FULLY_QUALIFIED_NAMESPACE"]
TOPIC_NAME = os.environ["SERVICEBUS_TOPIC_NAME"]
SUBSCRIPTION_NAME_G1 = 'Group1'
SUBSCRIPTION_NAME_G2 = 'Group2'
SUBSCRIPTION_NAME_G3 = 'Group3'
SUBSCRIPTION_NAME_VenueAll2 = 'VenueAll2' # for VenueAll2 
SUBSCRIPTION_NAME_VenueAll = 'venueAll' # for venueAll

Recevier_Session_ID_1 = 'Gr_1' # for Group 1
Recevier_Session_ID_2 = 'Gr_2' # for Group 2    

Recevier_Session_ID_3 = 'Gr_3' # for Group 3
#Recevier_Session_ID = 'Gr_3_1' # for Group 3
servicebus_client = ServiceBusClient.from_connection_string(
    conn_str=connstr_sb
)

# Receive a set of messages from a subscription under a topic.
def receive_messages(SUBSCRIPTION_NAME, Rxr_session_id=None):
    print(f'Trying to get messages. for {SUBSCRIPTION_NAME} - {Rxr_session_id}')
    with servicebus_client:
        receiver = servicebus_client.get_subscription_receiver(topic_name=TOPIC_NAME, 
                                                            subscription_name=SUBSCRIPTION_NAME,
                                                            session_id=Rxr_session_id,
                                                            #renew_session=True,
                                                            #receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE
                                                            ) 
        # receiver.session_id = "Gr_A" #Setting it this way will not work.Set it as above on the constructor.
        with receiver:
            received_msgs = receiver.receive_messages(max_message_count=50, max_wait_time=5)

            #received_msgs = receiver.receive_deferred_messages(sequence_numbers= [42784196460020024])
            print(f"Received {len(received_msgs)} messages. for {SUBSCRIPTION_NAME} - {Rxr_session_id}")
            for msg in received_msgs:
                print(str(msg))
                
                receiver.complete_message(msg)
                #receiver.defer_message(msg) # This will defer the message and put it back in the queue.
                # receiver.renew_message(msg) # This will renew the message lock and keep it in the queue. 
                #receiver.abandon_message(msg) # This will abandon the message and put it back in the queue.   

            receiver.close()
userinput = input("Press Any key to start,  Q to exit...")
while (userinput.capitalize() != "Q") :
    if (userinput.isnumeric()  and int(userinput) > 0):
        optionSelected = int(userinput)
        if optionSelected == 1:
            print("Option 1 selected")
            # Receive messages from the Subscription Group1 under Topic VenueA, which is session enabled
            receive_messages(SUBSCRIPTION_NAME_G1, Recevier_Session_ID_1)
        elif optionSelected == 2:
            print("Option 2 selected")
            # Receive messages from the Subscription Group2 under Topic VenueA, which is session enabled
            receive_messages(SUBSCRIPTION_NAME_G2, Recevier_Session_ID_2)
        elif optionSelected == 3:
            print("Option 3 selected")
            # Receive messages from the Subscription Group3 under Topic VenueA, which is session enabled
            receive_messages(SUBSCRIPTION_NAME_G3, Recevier_Session_ID_3)
        elif optionSelected == 4:
            print("Option 4 selected")
            # Receive messages from the Subscription VenueAll2 under Topic VenueA, which is session enabled
            receive_messages(SUBSCRIPTION_NAME_VenueAll2)
    else: # Any other key pressed, including Enter key, will receive messages from all subscriptions.
        receive_messages(SUBSCRIPTION_NAME_G1, Recevier_Session_ID_1)
        receive_messages(SUBSCRIPTION_NAME_G2, Recevier_Session_ID_2)
        receive_messages(SUBSCRIPTION_NAME_G3, Recevier_Session_ID_3)   
        # Receive messages from the Subscription VenueAll2 under Topic VenueA, which is sessionless
        #receive_messages(SUBSCRIPTION_NAME_VenueAll2)  
        #receive_messages(SUBSCRIPTION_NAME_VenueAll)  
        print("All receive messages are done.")
                
    userinput = input("Press Any Key to Receive Another Set of messages , or Q to exit...")
servicebus_client.close()
print("Service Bus Client is closed.")  
