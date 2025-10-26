"""Utility functions for the API."""
import json
import requests
from collections import defaultdict
from typing import Dict, List, Union
import logging

from config import config

logger = logging.getLogger(__name__)


def looks_like_int(value: str) -> bool:
    """
    Check if a string can be converted to an integer.
    
    Args:
        value: String value to check
        
    Returns:
        True if value can be converted to int, False otherwise
    """
    try:
        int(value)
        return True
    except (ValueError, TypeError):
        return False


def base36encode(number: int, alphabet: str = '0123456789abcdefghijklmnopqrstuvwxyz') -> str:
    """
    Convert an integer to a base36 string.
    
    Args:
        number: Integer to encode
        alphabet: Base alphabet to use (default is base36)
        
    Returns:
        Base36 encoded string
        
    Raises:
        TypeError: If number is not an integer
    """
    if not isinstance(number, int):
        raise TypeError('number must be an integer')

    base36 = ''
    sign = ''

    if number < 0:
        sign = '-'
        number = -number

    if 0 <= number < len(alphabet):
        return sign + alphabet[number]

    while number != 0:
        number, i = divmod(number, len(alphabet))
        base36 = alphabet[i] + base36

    return sign + base36


def base36decode(number: str) -> int:
    """
    Convert a base36 string to an integer.
    
    Args:
        number: Base36 encoded string
        
    Returns:
        Decoded integer value
    """
    return int(number, 36)


def get_submissions_from_es(ids: Union[str, List[str]]) -> Dict[int, Dict]:
    """
    Fetch submission data from Elasticsearch by IDs.
    
    Args:
        ids: Submission ID(s) as string or list of strings
        
    Returns:
        Dictionary mapping base10 IDs to submission data
    """
    if not isinstance(ids, (list, tuple)):
        ids = [ids]
    
    nested_dict = lambda: defaultdict(nested_dict)
    q = nested_dict()
    
    # Convert to integer IDs if needed
    int_ids = [int(id) if isinstance(id, str) else id for id in ids]
    q["query"]["terms"]["id"] = int_ids
    q["size"] = 1000
    
    primary_url, fallback_url = config.get_elasticsearch_urls()
    
    try:
        response = requests.get(
            f"{primary_url}/rs/submissions/_search",
            data=json.dumps(q),
            timeout=30
        )
        response.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Failed to get submissions from primary ES, trying fallback: {e}")
        try:
            response = requests.get(
                f"{fallback_url}/rs/submissions/_search",
                data=json.dumps(q),
                timeout=30
            )
            response.raise_for_status()
        except requests.RequestException as e2:
            logger.error(f"Failed to get submissions from fallback ES: {e2}")
            return {}
    
    s = json.loads(response.text)
    results = {}
    
    for hit in s.get("hits", {}).get("hits", []):
        source = hit["_source"]
        base_10_id = source.get("id")
        source["id"] = base36encode(int(hit["_id"]))
        results[base_10_id] = source
    
    return results


def get_submissions_from_pg(ids: Union[int, List[int]]) -> Dict[int, Dict]:
    """
    Fetch submission data from PostgreSQL by IDs.
    
    Args:
        ids: Submission ID(s) as integer or list of integers
        
    Returns:
        Dictionary mapping base10 IDs to submission data
    """
    # Import here to avoid circular dependency
    import DBFunctions
    
    if not isinstance(ids, (list, tuple)):
        ids = [ids]
    
    try:
        rows = DBFunctions.pgdb.execute(
            "SELECT * FROM submission WHERE (json->>'id')::int IN %s LIMIT 5000",
            tuple(ids)
        )
    except Exception as e:
        logger.error(f"Failed to get submissions from PostgreSQL: {e}")
        return {}
    
    results = {}
    
    if rows:
        for row in rows:
            submission = row[0]
            base_10_id = submission['id']
            submission['id'] = base36encode(submission['id'])
            
            if 'subreddit_id' in submission:
                submission['subreddit_id'] = "t5_" + base36encode(submission['subreddit_id'])
            
            submission.pop('name', None)
            results[base_10_id] = submission
    
    return results