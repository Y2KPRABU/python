import logging
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            "Invalid JSON",
            status_code=400
        )

    # Process the webhook payload
    logging.info(f"Received webhook payload: {req_body}")

    return func.HttpResponse(
        "Webhook received successfully",
        status_code=200
    )
