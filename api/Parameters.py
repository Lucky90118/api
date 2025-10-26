"""Query parameter processing for Elasticsearch queries."""
import json
import time
from collections import defaultdict
from typing import Dict, Any, Tuple, List
import logging

from Helpers import looks_like_int
from logger_config import default_logger

logger = default_logger


def nested_dict():
    """Create a nested defaultdict for building Elasticsearch queries."""
    return defaultdict(nested_dict)


def process(params: Dict[str, Any], query: defaultdict) -> Tuple[Dict[str, Any], defaultdict]:
    """
    Process and normalize query parameters for Elasticsearch.
    
    Args:
        params: Raw request parameters
        query: Elasticsearch query dict
        
    Returns:
        Tuple of (processed_params, elasticsearch_query)
    """
    # Lowercase all parameter names
    params = {k.lower(): v for k, v in params.items()}
    suggested_sort = "desc"

    # Filter by subreddit and author
    conditions = ["subreddit", "author"]
    for condition in conditions:
        if condition in params and params[condition] is not None:
            terms = nested_dict()
            if not isinstance(params[condition], (list, tuple)):
                params[condition] = [params[condition]]
            params[condition] = [x.lower() for x in params[condition]]
            terms['terms'][condition] = params[condition]
            query['query']['bool']['filter'].append(terms)

    # Time range filtering
    params = _process_time_range(params, query)
    
    # Score filtering
    params = _process_score_filter(params, query)
    
    # Comment count filtering
    params = _process_comment_count_filter(params, query)
    
    # Boolean filters
    params = _process_boolean_filters(params, query)

    # Set sort_type default
    if 'sort_type' in params and params['sort_type'] is not None:
        params["sort_type"] = params['sort_type'].lower()
    else:
        params["sort_type"] = "created_utc"

    # Support 'limit' as alias for 'size'
    if 'limit' in params:
        params['size'] = params['limit']

    # Validate and set size
    if 'size' in params and params['size'] is not None and looks_like_int(params['size']):
        size = min(500, int(params['size']))  # Cap at 500
        query['size'] = params['size'] = size
    else:
        query['size'] = params['size'] = 25

    # Support 'order' as alias for 'sort'
    if 'order' in params and params['order'] is not None:
        params['sort'] = params['order'].lower()

    # Set sort direction
    if 'sort' in params and params['sort'] is not None:
        params['sort'] = params['sort'].lower()
    else:
        params['sort'] = suggested_sort
    
    query['sort'][params['sort_type']] = params['sort']

    # Validate frequency parameter
    if 'frequency' in params and params['frequency'].lower() in ['second', 'minute', 'hour', 'day', 'week', 'month']:
        params['frequency'] = params['frequency'].lower()
    else:
        params['frequency'] = None

    return params, query


def _process_time_range(params: Dict[str, Any], query: defaultdict) -> Dict[str, Any]:
    """Process time range parameters (after/before)."""
    # After parameter
    if 'after' in params and params['after'] is not None:
        params['after'] = _parse_time_value(params['after'])
        range_filter = nested_dict()
        range_filter['range']['created_utc']['gt'] = params['after']
        query['query']['bool']['filter'].append(range_filter)
        params['after'] = params['after']  # Keep for metadata
    else:
        params['after'] = None

    # Before parameter
    if 'before' in params and params['before'] is not None:
        params['before'] = _parse_time_value(params['before'])
        range_filter = nested_dict()
        range_filter['range']['created_utc']['lt'] = params['before']
        query['query']['bool']['filter'].append(range_filter)
        params['before'] = params['before']  # Keep for metadata
    else:
        params['before'] = None
    
    return params


def _process_score_filter(params: Dict[str, Any], query: defaultdict) -> Dict[str, Any]:
    """Process score filtering parameter."""
    if 'score' in params and params['score'] is not None:
        range_filter = nested_dict()
        score = params['score']
        
        if score.startswith("<"):
            range_filter['range']['score']['lt'] = int(score[1:])
        elif score.startswith(">"):
            range_filter['range']['score']['gt'] = int(score[1:])
        elif looks_like_int(score):
            range_filter['term']['score'] = int(score)
        
        query['query']['bool']['filter'].append(range_filter)
    
    return params


def _process_comment_count_filter(params: Dict[str, Any], query: defaultdict) -> Dict[str, Any]:
    """Process num_comments filtering parameter."""
    if 'num_comments' in params and params['num_comments'] is not None:
        range_filter = nested_dict()
        num_comments = params['num_comments']
        
        if num_comments.startswith("<"):
            range_filter['range']['num_comments']['lt'] = int(num_comments[1:])
        elif num_comments.startswith(">"):
            range_filter['range']['num_comments']['gt'] = int(num_comments[1:])
        elif looks_like_int(num_comments):
            range_filter['term']['num_comments'] = int(num_comments)
        
        query['query']['bool']['filter'].append(range_filter)
    
    return params


def _process_boolean_filters(params: Dict[str, Any], query: defaultdict) -> Dict[str, Any]:
    """Process boolean filter parameters."""
    conditions = ["over_18", "is_video", "stickied", "spoiler", "locked", "contest_mode"]
    
    for condition in conditions:
        if condition in params and params[condition] is not None:
            parameter = nested_dict()
            value = params[condition]
            
            if value.lower() == 'true' or value == "1":
                parameter['term'][condition] = "true"
            elif value.lower() == 'false' or value == "0":
                parameter['term'][condition] = "false"
            else:
                logger.warning(f"Invalid boolean value for {condition}: {value}")
                continue
            
            query['query']['bool']['filter'].append(parameter)
    
    return params


def _parse_time_value(time_value: str) -> int:
    """
    Parse time value from string to epoch timestamp.
    
    Supports:
    - Epoch timestamp (integer string)
    - Relative time: {number}{unit} (e.g., 30d, 24h, 7m, 3600s)
    """
    if looks_like_int(time_value):
        return int(time_value)
    
    if not time_value or len(time_value) < 2:
        raise ValueError(f"Invalid time format: {time_value}")
    
    unit = time_value[-1].lower()
    try:
        value = int(time_value[:-1])
    except ValueError:
        raise ValueError(f"Invalid time format: {time_value}")
    
    now = int(time.time())
    
    if unit == "d":
        return now - (value * 86400)
    elif unit == "h":
        return now - (value * 3600)
    elif unit == "m":
        return now - (value * 60)
    elif unit == "s":
        return now - value
    else:
        raise ValueError(f"Unknown time unit: {unit}")