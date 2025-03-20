import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
import azure.cosmos.http_constants as http_constants
import azure.cosmos.documents as documents
import azure.cosmos.errors as errors
from azure.cosmos import PartitionKey
from cosmosutil import *

# Initialize the Cosmos client

# Create a database
DATABASE_ID = 'SchoolDatabase'
client = cosmos_client.CosmosClient(HOST, {'masterKey': MASTER_KEY})

# Create a container
CONTAINER_ID = 'Students'
try:
   # setup database for this sample
    db = client.create_database_if_not_exists(id=DATABASE_ID)
        # setup container for this sample
    container = db.create_container_if_not_exists(id=CONTAINER_ID,
                                                      partition_key=PartitionKey(path='/studentId', kind='Hash'))

    print(f"Container created or returned: {container.id}")
except exceptions.CosmosHttpResponseError as e:
        print('\nrun_sample has caught an error. {0}'.format(e.message))

finally:
        print("\nrun_sample done")

# Function to add a student record
def add_student(student_id, student_name, student_age):
    student_record = {
        'id': student_id,
        'studentId': student_id,
        'name': student_name,
        'age': student_age
    }
    try:
        container.create_item(body=student_record)
        print(f'Student {student_name} added successfully.')
    except exceptions.CosmosHttpResponseError as e:
        print(f'An error occurred: {e.message}')

# Example usage
#add_student('1', 'John Doe', 20)
#add_student('2', 'John Mason', 19)
add_student('3', 'Sun Mason', 19)