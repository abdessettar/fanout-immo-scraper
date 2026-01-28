import json
import logging
from datetime import datetime
import os
import time
from typing import Dict, Any

from utils import (
    create_session_with_ip_rotation, 
    shutdown_gateway, 
    chunk_list, 
    BASE_URL, 
    sqs_client
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


PAGE_BATCH_QUEUE_URL = os.getenv("PAGE_BATCH_QUEUE_URL")
PAGE_BATCH_SIZE      = int(os.getenv("PAGE_BATCH_SIZE", 120)) # A adapter en fonction de la performance du scraper

TRANSACTION_TYPES = [
    "maison/a-vendre",
    "maison/a-louer",
    "appartement/a-vendre",
    "appartement/a-louer"
]

# Each page has 30 items.
# TODO: add a check to verify this assumption and/or make it dynamic (in case the website changes).
ITEMS_PER_PAGE = 30

def get_total_pages(session, gateway,transaction_type: str) -> int:

    url = f"{BASE_URL}/fr/search-results/{transaction_type}?page=1"
    logger.info(f"Fetching total page count from: {url}")
    

    response = session.get(url)
    nbr_retries_first_page = 1  # To track performance, we count the number of retries
    while response.status_code != 200:
        print(f'Retry number {nbr_retries_first_page} for first page.')
        # Log the IP and region that failed out of curiosity
        logger.warning(f"IP {response.headers.get('X-Amz-Cf-Pop')} from region {gateway.regions[0]} failed with code {response.status_code}")
        # Close the session and gateway to force a rotation. We sleep a bit as it can take some time:
        if session and gateway:
            session.close()
            gateway.close()
            time.sleep(1.5)

        session, gateway = create_session_with_ip_rotation()

        response = session.get(url)
        nbr_retries_first_page += 1
    
    data = response.json()
    total_items = data.get("totalItems", 9969)
    if "totalItems" not in data:
        logger.warning(f"totalItems not found in response. Fall back to 9969. Response: {data}")
    
    return (total_items // ITEMS_PER_PAGE) + 1

def lambda_handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:

    start_time = datetime.now()
    logger.info(f"--- Starting scraper execution for event: {event} ---")
    logger.info(f"              Started at {start_time}                ")
    gateway = None
    session = None
    
    if not PAGE_BATCH_QUEUE_URL:
        raise ValueError("Environment variable PAGE_BATCH_QUEUE_URL is not set.")

    active_gateways = [] # Track resources for cleanup

    try:
        total_messages_sent = 0

        for trans_type in TRANSACTION_TYPES:
            logger.info(f"Processing transaction type: {trans_type}")
            
            # Check if we are running out of time and still have more than 30 seconds left (i.e., we have enough time to close the gateway)
            if context.get_remaining_time_in_millis() < 30000:
                logger.error("Approaching timeout, stopping early to clean up :(")
                break

            try:
                session, gateway = create_session_with_ip_rotation()
                active_gateways.append(gateway)
                num_pages = get_total_pages(session, gateway, trans_type)
                logger.info(f"Found {num_pages} pages for {trans_type}.")
                
                page_numbers = list(range(1, num_pages + 1))
                page_batches = chunk_list(page_numbers, PAGE_BATCH_SIZE)
                
                for batch in page_batches:
                    message_body = {
                        "transaction_type": trans_type,
                        "page_numbers": batch
                    }
                    sqs_client.send_message(
                        QueueUrl=PAGE_BATCH_QUEUE_URL,
                        MessageBody=json.dumps(message_body)
                    )
                logger.info(f"Sent {len(page_batches)} batches to SQS for {trans_type}.")
                total_messages_sent += len(page_batches)

                shutdown_gateway(gateway)
                active_gateways.remove(gateway)

            except Exception as e:
                logger.error(f"Failed to process {trans_type}: {e}", exc_info=True)
                if gateway in active_gateways:
                    shutdown_gateway(gateway)
                    active_gateways.remove(gateway)
                continue
        
        end_time = datetime.now()
        logger.info(f"Function's run ended at {end_time}                ")
        logger.info(f"It took {end_time - start_time}")
        return {"statusCode": 200, "body": f"Successfully dispatched {total_messages_sent} page batches."}

    # except Exception as e:
    #     # This will cause the function to fail and potentially be retried by AWS: we don't want that for now, go for the finally block...
    #     logger.error("A fatal error occurred in the dispatcher.", exc_info=True)
    #     raise
    finally:
        if session:
            session.close()
        for gate in active_gateways:
            shutdown_gateway(gate)