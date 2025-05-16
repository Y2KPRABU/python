import asyncio

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
from azure_appconfig import get_azure_appconfig_value

# Get the Event hub  connection string from Azure App Configuration
connstr_ehub = get_azure_appconfig_value('conn_sunehub_group1')
EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = "sunehub.servicebus.windows.net"
EVENT_HUB_NAME = "group1"

print(f"Event Hub Client is created. {EVENT_HUB_FULLY_QUALIFIED_NAMESPACE} - {EVENT_HUB_NAME}")
async def run():
    # Create a producer client to send messages to the event hub.
    producer = EventHubProducerClient.from_connection_string(connstr_ehub)
    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData("First event "))
        event_data_batch.add(EventData("Second event"))
        event_data_batch.add(EventData("Third event"))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)
        print("Batch of events is sent to the event hub.")
asyncio.run(run())