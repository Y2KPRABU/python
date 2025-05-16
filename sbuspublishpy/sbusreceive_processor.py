import os
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure_appconfig import get_servicebus_connection_string
# Get the Service Bus connection string from Azure App Configuration
connstr_sb = get_servicebus_connection_string()
os.environ["SERVICE_BUS_CONNECTION_STR"] = connstr_sb
connstr_sb = os.getenv("SERVICE_BUS_CONNECTION_STR")
os.environ["SERVICEBUS_TOPIC_NAME"]='venueb'
TOPIC_NAME = os.environ["SERVICEBUS_TOPIC_NAME"]


#Grouping all consants in a single place
SUBSCRIPTION_NAME_G1 = 'Group1'
SUBSCRIPTION_NAME_G1 = 'Group1'
SUBSCRIPTION_NAME_G2 = 'Group2'
SUBSCRIPTION_NAME_G3 = 'Group3'
SUBSCRIPTION_NAME_VenueAll2 = 'VenueAll2' # for VenueAll2 
SUBSCRIPTION_NAME_VenueAll = 'venueAll' # for venueAll

Recevier_Session_ID_1 = 'Gr_1' # for Group 1
Recevier_Session_ID_2 = 'Gr_2' # for Group 2    

Recevier_Session_ID_3 = 'Gr_3' # for Group 3
#Recevier_Session_ID = 'Gr_3_1' # for Group 3
receiver = None
servicebus_client = ServiceBusClient.from_connection_string(
        conn_str=connstr_sb)
    
def init_receiver(SUBSCRIPTION_NAME, Rxr_session_id=None):
    
    receiver = servicebus_client.get_subscription_receiver(topic_name=TOPIC_NAME, 
                                                            subscription_name=SUBSCRIPTION_NAME,
                                                            session_id=Rxr_session_id,
                                                            renew_session=True,
                                                            max_wait_time=50)
    return  receiver

async def async_receive_messages_continuous(SUBSCRIPTION_NAME, Rxr_session_id=None):
            receiver = servicebus_client.get_subscription_receiver(topic_name=TOPIC_NAME, 
                                                            subscription_name=SUBSCRIPTION_NAME,
                                                            session_id=Rxr_session_id,
                                                            #renew_session=True,
                                                            max_wait_time=5)
            #session = receiver.session
            #print(session.locked_until_utc)
            print(f"Waiting to get messages. for {SUBSCRIPTION_NAME} - {Rxr_session_id}")
            # Receive a set of messages from a subscription under a topic.
            async for message in receiver:
                print(f"Received message from {SUBSCRIPTION_NAME} - {Rxr_session_id} - {message}")    
                await receiver.complete_message(message)
                #await session.renew_lock()


userinput = input("Press Any key to start,  Q to exit...")
while (userinput.capitalize() != "Q") :
    if (userinput.isnumeric()  and int(userinput) > 0):
        optionSelected = int(userinput)
        if optionSelected == 1:
            print("Option 1 selected")
            #receiver = init_receiver(SUBSCRIPTION_NAME_G1, Recevier_Session_ID_1)
            # Receive messages from the Subscription Group1 under Topic VenueA, which is session enabled
            asyncio.run(async_receive_messages_continuous(SUBSCRIPTION_NAME_G1, Recevier_Session_ID_1))
        elif optionSelected == 2:
            print("Option 2 selected")
            # Receive messages from the Subscription Group2 under Topic VenueA, which is session enabled
            asyncio.run(async_receive_messages_continuous(SUBSCRIPTION_NAME_G2, Recevier_Session_ID_2))
        elif optionSelected == 3:
            print("Option 3 selected")
            # Receive messages from the Subscription Group3 under Topic VenueA, which is session enabled
            asyncio.run(async_receive_messages_continuous(SUBSCRIPTION_NAME_G3, Recevier_Session_ID_3))
        elif optionSelected == 4:
            print("Option 4 selected")
            # Receive messages from the Subscription VenueAll2 under Topic VenueA, which is session enabled
            asyncio.run( async_receive_messages_continuous(SUBSCRIPTION_NAME_VenueAll))
    else: # Any other key pressed, including Enter key, will receive messages from all subscriptions.
        #async_receive_messages_continuous(SUBSCRIPTION_NAME_G1, Recevier_Session_ID_1)
        #async_receive_messages_continuous(SUBSCRIPTION_NAME_G2, Recevier_Session_ID_2)
        #async_receive_messages_continuous(SUBSCRIPTION_NAME_G3, Recevier_Session_ID_3)   
        # Receive messages from the Subscription VenueAll2 under Topic VenueA, which is sessionless
        #receive_messages(SUBSCRIPTION_NAME_VenueAll2)  
        #receive_messages(SUBSCRIPTION_NAME_VenueAll)  
        print("All receive messages are done.")
                
    userinput = input("Press Any Key to Receive Another Set of messages , or Q to exit...")

#receiver.close()
#servicebus_client.close()

print("Service Bus Client is closed.")  
#if __name__ == "__main__":
    #asyncio.run(receive_messages())
