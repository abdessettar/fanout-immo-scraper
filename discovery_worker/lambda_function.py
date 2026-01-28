import json
import logging
import os
import time
import random
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError
import requests
from requests.adapters import HTTPAdapter, Retry
from requests_ip_rotator import ApiGateway


from utils import (
    create_session_with_ip_rotation,
    chunk_list,
    BASE_URL,
    sqs_client,
    s3_client,
    ssm_client,
    update_ssm_parameter,
    get_ssm_parameter,
    shutdown_gateway
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


ID_BATCH_QUEUE_URL = os.getenv("ID_BATCH_QUEUE_URL")
ID_BATCH_SIZE = int(os.getenv("ID_BATCH_SIZE", 100))
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


def scrape_search_page_with_retries(
    session: requests.Session,
    gateway: ApiGateway,
    url: str,
    page_num: int
) -> List[Dict]:
    """
    #TODO: move it to utils
    """
    nbr_retries = 0
    max_retries = 5 #10

    while nbr_retries < max_retries:
        try:
            response = session.get(url, timeout=10)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    return data.get("results", [])
                except json.JSONDecodeError:
                    logger.warning(f"JSON Decode Error on page {page_num}")
                    break 
            
            elif response.status_code in [403, 429]:
                logger.warning(f"Rate limited on page {page_num} with code {response.status_code}. Sleeping...zzZ")
                time.sleep(random.uniform(2, 5))
            else:
                logger.warning(f"Failed page {page_num} with code {response.status_code}")
                
        except requests.RequestException as e:
            logger.warning(f"Network error on page {page_num}: {e}")
            time.sleep(random.uniform(2, 3))

        nbr_retries += 1
    
    # If we exit the loop, we failed and raise an exception to add this specific ID to batchItemFailures
    raise Exception(f"Failed to scrape page {page_num} after {max_retries} attempts")



def process_record(record: Dict[str, Any], session: requests.Session) -> None:
    """
    #TODO: 
    #   - move it to utils
    #   - break it down into smaller functions: doing too much rn
    """
    message_body        = json.loads(record['body'])
    transaction_type    = message_body['transaction_type']
    page_numbers        = message_body['page_numbers']
    
    logger.info(f"Processing {len(page_numbers)} pages for {transaction_type}, starting with page {page_numbers[0]}.")

    all_ids_in_batch = []
    snapshot_data_for_batch = {}
    
    for page_num in page_numbers:
        url = f"{BASE_URL}/fr/search-results/{transaction_type}?page={page_num}"
        
        results = scrape_search_page_with_retries(session, url, page_num)
        
        if not results:
            logger.warning(f"No results found for search page {page_num} :(")
            continue

        for result in results:
            ad_id = result.get("id", None)
            if ad_id:
                all_ids_in_batch.append(ad_id)
                snapshot_data_for_batch[ad_id] = result
    
    if snapshot_data_for_batch:
        timestamp = datetime.now().strftime('%H%M%S%m%d%Y')
        random_suffix = random.randint(1000, 9999)
        s3_key = f"snapshots/{transaction_type}/{transaction_type.replace('/', '-')}_{random_suffix}_{timestamp}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(snapshot_data_for_batch, indent=2),
            ContentType="application/json"
        )
        logger.info(f"Uploaded {len(snapshot_data_for_batch)} elements in snapshot: {s3_key}")

    ssm_parameter_name = f"/{transaction_type}"
    last_known_max_id = get_ssm_parameter(ssm_parameter_name)
    
    if all_ids_in_batch:
        current_max_id = max(all_ids_in_batch)
        
        new_ids = [x for x in all_ids_in_batch if x > last_known_max_id]

        if new_ids:
            id_batches = chunk_list(new_ids, ID_BATCH_SIZE)
            for batch in id_batches:
                sqs_message = {
                    "transaction_type": transaction_type, 
                    "listing_ids": batch
                }
                sqs_client.send_message(
                    QueueUrl=ID_BATCH_QUEUE_URL,
                    MessageBody=json.dumps(sqs_message)
                )
            logger.info(f"Sent {len(new_ids)} new IDs to Worker Queue.")
        
        if current_max_id > last_known_max_id:
            update_ssm_parameter(ssm_parameter_name, current_max_id)


def lambda_handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:
    
    start_time = datetime.now()
    batch_item_failures = []
    
    gateway = None
    session = None
    
    if not all([ID_BATCH_QUEUE_URL, S3_BUCKET_NAME]):
        logger.error("Missing configuration.")
        raise ValueError("Missing ID_BATCH_QUEUE_URL or S3_BUCKET_NAME.")

    try:
        session, gateway = create_session_with_ip_rotation()
        
        for record in event['Records']:
            try:
                if context.get_remaining_time_in_millis() < 10000:
                    logger.warning("Lambda approaching timeout. Marking remaining records as failed.")
                    batch_item_failures.append({"itemIdentifier": record['messageId']})
                    continue

                process_record(record, session)
                
            except Exception as e:
                logger.error(f"Failed to process message {record['messageId']}: {e}", exc_info=True)
                batch_item_failures.append({"itemIdentifier": record['messageId']})

    except Exception as e:
        # If the Gateway creation itself fails, we must fail the whole batch
        logger.error(f"Fatal infrastructure/network error", exc_info=True)
        raise e
        
    finally:
        if gateway:
            shutdown_gateway(gateway)
        if session:
            session.close()
            
    end_time = datetime.now()
    logger.info(f"Execution finished in {end_time - start_time}. Failures: {len(batch_item_failures)}")

    # return the list of failed IDs to SQS for retry
    return {
        "batchItemFailures": batch_item_failures
    }