import json
import logging
import os
import time
import random
from datetime import datetime
from typing import Dict, Any, Optional

import boto3
from botocore.exceptions import ClientError
import requests
from requests_ip_rotator import ApiGateway

from utils import (
    create_session_with_ip_rotation,
    shutdown_gateway,
    BASE_URL,
    s3_client
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


def scrape_ad_details(
    session: requests.Session,
    ad_id: int
) -> Optional[Dict]:
    
    url = f"{BASE_URL}/fr/classified/get-result/{ad_id}"
    
    max_retries = 3 #5
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=5)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    return data.get("classified")
                except json.JSONDecodeError:
                    logger.warning(f"JSON Decode Error for ID {ad_id}")
                    return None
            
            elif response.status_code == 404:
                logger.info(f"Ad {ad_id} not found (404). The listing might have been deleted after being scraped by the batch crawler.")
                return None
                
            elif response.status_code in [403, 429]:
                # Rate limited: take a breake and sleep briefly, but don't kill the gateway (not yet... let it live another session)
                time.sleep(random.uniform(1, 3))
            
            else:
                logger.warning(f"Ad {ad_id} returned status {response.status_code} :(")
                
        except requests.RequestException as e:
            logger.warning(f"Network error for ID {ad_id}: {e}")
            time.sleep(1)
            
    # If we get here, we failed after retries (:/)
    raise Exception(f"Failed to fetch ID {ad_id} after {max_retries} attempts :'( ")


def process_record(record: Dict[str, Any], session: requests.Session) -> None:
    """
    TODO: create a upload_to_s3 function in utils.py. This function does too much.
    """
    message_body            = json.loads(record['body'])
    transaction_type        = message_body.get('transaction_type', 'unknown')
    ad_ids                  = message_body.get('listing_ids', [])
    
    logger.info(f"Processing batch of {len(ad_ids)} IDs for {transaction_type}")
    
    batch_results = {}
    
    for ad_id in ad_ids:
        try:
            ad_data = scrape_ad_details(session, ad_id)
            if ad_data:
                batch_results[str(ad_id)] = ad_data
        except Exception as e:
            logger.error(f"Failed to scrape ID {ad_id}: {e}")
            continue

    if batch_results:
        timestamp = datetime.now().strftime('%H%M%S%m%d%Y') #'%Y-%m-%d_%H-%M-%S'
        random_suffix = random.randint(1000, 9999)
        s3_key = f"{transaction_type}/{transaction_type.replace('/', '-')}_{random_suffix}_{timestamp}.json"
        
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(batch_results),
                ContentType="application/json"
            )
            logger.info(f"Uploaded {len(batch_results)} ads to {s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise e


def lambda_handler(event: Dict[str, Any], context: object) -> Dict[str, Any]:
    batch_item_failures = []
    session = None
    gateway = None

    if not S3_BUCKET_NAME:
        raise ValueError("S3_BUCKET_NAME is missing")

    try:
        session, gateway = create_session_with_ip_rotation()
        
        for record in event['Records']:
            try:
                if context.get_remaining_time_in_millis() < 10000:
                    logger.warning("Approaching timeout, failing remaining records.")
                    batch_item_failures.append({"itemIdentifier": record['messageId']})
                    continue

                process_record(record, session)
                
            except Exception as e:
                logger.error(f"Failed to process SQS message {record['messageId']}: {e}", exc_info=True)
                batch_item_failures.append({"itemIdentifier": record['messageId']})
                
    except Exception as e:
        logger.error("Fatal Infrastructure Error", exc_info=True)
        raise e
        
    finally:
        if gateway:
            shutdown_gateway(gateway)
        if session:
            session.close()

    return {"batchItemFailures": batch_item_failures}