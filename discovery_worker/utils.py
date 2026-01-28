import logging
import os
import random
from typing import Tuple, List

import boto3
import requests
from requests.adapters import HTTPAdapter, Retry
from requests_ip_rotator import ApiGateway

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("ACCESS_KEY_SECRET")

BASE_URL = "https://www.immoweb.be"

REQUEST_TIMEOUT = 10
RETRY_STATUS_FORCELIST = [403, 429, 500, 502, 503, 504]
RETRY_TOTAL = 5

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")
ssm_client = boto3.client("ssm")


def create_session_with_ip_rotation() -> Tuple[requests.Session, ApiGateway]:

    logger.info("Creating a new API Gateway for IP rotation...")
    USER_AGENTS = [
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0',
        'Opera/9.80 (Linux mips ; U; HbbTV/1.1.1 (; Philips; ; ; ; ) CE-HTML/1.0 NETTV/3.2.4; en) Presto/2.6.33 Version/10.70',
        'Mozilla/5.0 (Windows; U; Windows NT 6.0; de; rv:1.9.2.20) Gecko/20110803 Firefox/3.6.19',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 OPR/77.0.4054.203',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 OPR/121.0.0.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0'
        ]
    ROTATOR_REGIONS = [
        #'us-west-2', 'eu-west-1', 'eu-west-2', 'eu-west-3',  
        'ap-southeast-2', 'ap-northeast-1', 'sa-east-1',
        'us-east-2', 'us-west-1', 'us-east-1', 
        'eu-north-1',
        'eu-central-1', 'ca-central-1', 'ap-south-1',
        'ap-northeast-3', 'ap-northeast-2', 'ap-southeast-1'
    ]

    la_region = random.choice(ROTATOR_REGIONS)
    client_apigateway = boto3.client('apigateway', region_name=la_region)

    gateway = ApiGateway(
        site=BASE_URL,
        access_key_id=ACCESS_KEY_ID,
        access_key_secret=SECRET_ACCESS_KEY,
        regions=[la_region],
        verbose=False
    )
    gateway.start(force=False)
    logger.info(f"API Gateway started in region {la_region}.")

    session = requests.Session()
    session.mount(BASE_URL, gateway)

    retry_strategy = Retry(
        total=RETRY_TOTAL,
        backoff_factor=1,
        status_forcelist=RETRY_STATUS_FORCELIST,
        allowed_methods=["GET"]
    )

    session.headers.update({
        # TODO: add referer from the search page for more stealth to decrease the chance of being blocked
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'application/json',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.google.com/',
        'Connection': 'keep-alive',
    })

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('https://', adapter)

    return session, gateway

def shutdown_gateway(gateway: ApiGateway):
    if gateway:
        logger.info("Shutting down API Gateway...")
        gateway.shutdown()
        logger.info("API Gateway shut down successfully.")

def chunk_list(data: list, size: int) -> List[list]:
    if not data:
        return []
    return [data[i:i + size] for i in range(0, len(data), size)]


def get_ssm_parameter(parameter_name: str) -> int:
    try:
        response = ssm_client.get_parameter(Name=parameter_name, WithDecryption=True)
        value = int(response["Parameter"]["Value"])
        logger.info(f"Successfully retrieved SSM parameter '{parameter_name}' with value: {value}")
        return value
    except ClientError as e:
        if e.response["Error"]["Code"] == "ParameterNotFound":
            logger.warning(f"SSM parameter '{parameter_name}' not found. Defaulting to 0.")
            return 0
        else:
            logger.error(f"Error retrieving SSM parameter '{parameter_name}': {e}")
            raise
    except (ValueError, TypeError) as e:
        logger.error(f"SSM parameter value for '{parameter_name}' is not a valid integer. Error: {e}")
        raise

def update_ssm_parameter(parameter_name: str, value: int):
    try:
        ssm_client.put_parameter(
            Name=parameter_name,
            Value=str(value),
            Type="String",
            Overwrite=True,
        )
        logger.info(f"Successfully updated SSM parameter '{parameter_name}' to {value}.")
    except ClientError as e:
        logger.error(f"Error updating SSM parameter '{parameter_name}': {e}")
        raise