import azure.functions as func
import logging
import json
import requests

app = func.FunctionApp()
@app.cosmos_db_trigger(arg_name="studentslist", container_name="Students",database_name="SchoolDatabase", 
                       connection="conn_cosmosdb_DOCUMENTDB",lease_container_name="leases_student",
                       create_lease_container_if_not_exists="true", lease_renew_interval=50000 )  
def cosmosdb_trigger(studentslist: func.DocumentList):
    logging.info('Python fn Sundar CosmosDB triggered by Students.')
    try:
        if studentslist:
            logging.info('No of Student Documents received is %s', studentslist.count)
            for document in studentslist:
                logging.info('Processing document: %s', document['id'])
                logging.warning('Processing document name : %s', document['name'])
                logging.info('Processing document studentid : %s', document['studentId'])
                 # Serialize documents to JSON
                serialized_docs = [json.dumps(doc.to_dict()) for doc in studentslist]
                logging.info('Processing document tostring : %s', serialized_docs)
                invoke_another_child(serialized_docs)
                logging.info('Processing document child called successfully')
    except Exception as e:
        logging.error('Error processing documents: %s', str(e))
        raise
    finally:
        logging.info('Completed processing documents')
    logging.info('Completed processing documents')
    
def invoke_another_child(studentJson:str) -> func.HttpResponse:
    # This function is a placeholder for invoking another child function
    # You can implement the logic to invoke another function or service here
    logging.info('Python HTTP  function child function invoke received.')

    # URL of the API you want to invoke
    api_url = 'https://studentrxr.azurewebsites.net/todos/1'

    try:
        # Making a GET request to the external API
        #response = requests.get(api_url)
            
        logging.info('Input param is function invoke received. : %s',studentJson)
        for student in studentJson:
            logging.info('Processing document: %s', student)

            response = requests.post(api_url,data=student, headers={'Content-Type': 'application/json'})
            logging.info('called post api success  for student ' )

            response.raise_for_status()  # Raise an exception for HTTP errors

        # Process the response from the external API
            api_data = response.json()

        # Return the data from the external API as the response of the Azure Function
        return func.HttpResponse(
            body=str(api_data),
            status_code=200
        )
    except requests.exceptions.RequestException as e:
        logging.error(f"Error invoking API: {e}")
        return func.HttpResponse(
            body="Failed to invoke external API.",
            status_code=500
        )
