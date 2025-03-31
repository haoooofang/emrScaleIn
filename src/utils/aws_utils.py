import boto3
import logging

logger = logging.getLogger('emr_scaling')

def create_aws_client(service_name, region_name, **kwargs):
    """
    Create an AWS service client with proper error handling.
    
    Args:
        service_name: AWS service name (e.g., 'emr', 'cloudwatch')
        region_name: AWS region name
        **kwargs: Additional arguments to pass to boto3.client
        
    Returns:
        AWS service client
    """
    try:
        client = boto3.client(service_name, region_name=region_name, **kwargs)
        logger.debug(f"Created {service_name} client in {region_name}")
        return client
    except Exception as e:
        logger.error(f"Failed to create {service_name} client: {str(e)}")
        raise

def retry_aws_api(func, max_retries=3, initial_backoff=1):
    """
    Decorator to retry AWS API calls with exponential backoff.
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retries
        initial_backoff: Initial backoff time in seconds
        
    Returns:
        Decorated function
    """
    import time
    import functools
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        retries = 0
        backoff = initial_backoff
        
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries > max_retries:
                    logger.error(f"Max retries ({max_retries}) exceeded for {func.__name__}: {str(e)}")
                    raise
                
                logger.warning(f"Retry {retries}/{max_retries} for {func.__name__} after error: {str(e)}")
                time.sleep(backoff)
                backoff *= 2  # Exponential backoff
    
    return wrapper
