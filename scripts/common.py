"""
Common utilities for data ingestion scripts.
"""

import time
import requests
import duckdb
from typing import Callable, Tuple, Any
from functools import wraps


def retry(max_retries: int = 5, backoff: int = 5):
    """
    Decorator that adds retry logic to functions returning (bool, Any).
    
    Args:
        max_retries: Maximum retry attempts (default: 5)
        backoff: Base seconds for linear backoff (default: 5)
    
    Returns:
        Decorated function that retries on failure
    
    Example:
        @retry(max_retries=3, backoff=5)
        def download_and_insert(date_str, path, uri):
            # ... your logic ...
            return True, file_size
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Tuple[bool, Any]:
            for attempt in range(1, max_retries + 1):
                try:
                    success, result = func(*args, **kwargs)
                    if success:
                        return True, result
                    return False, result
                    
                except (requests.RequestException, duckdb.Error) as e:
                    print(f"Error on attempt {attempt}/{max_retries}: {e}")
                    if attempt == max_retries:
                        print(f"Failed after {max_retries} attempts")
                        return False, None
                    wait_time = backoff * attempt
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    
                except Exception as e:
                    print(f"Unexpected error (no retry): {e}")
                    return False, None
            
            return False, None
        return wrapper
    return decorator