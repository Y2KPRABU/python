import azure.functions as func
import logging

app = func.FunctionApp()
@app.cosmos_db_trigger(arg_name="azcosmosdb", container_name="Students",database_name="SchoolDatabase", 
                       connection="conn_cosmosdb_DOCUMENTDB",lease_container_name="leases_student",
                       create_lease_container_if_not_exists="true")  
def cosmosdb_trigger(azcosmosdb: func.DocumentList):
    logging.info('Python CosmosDB triggered.')
    if azcosmosdb:
        logging.info('Document Id: %s', azcosmosdb[0].get('id'))
        for document in azcosmosdb:
            logging.info('Processing document: %s', document['id'])
