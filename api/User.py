"""User analysis handler for Reddit API."""
import time
import json
import requests
from collections import defaultdict
from typing import Dict, Any, Optional
import logging

from Helpers import base36encode
from config import config
from logger_config import default_logger

logger = default_logger


class UserAnalyzer:
    """Handler for user activity analysis."""
    
    def __init__(self):
        """Initialize the user analyzer."""
        self.es_primary, self.es_fallback = config.get_elasticsearch_urls()
        self.es_index = "/rc/comments/_search"
    
    def on_get(self, req, resp, author: str):
        """
        Analyze user activity patterns.
        
        Args:
            req: Falcon request object
            resp: Falcon response object
            author: Reddit username
        """
        start_time = time.time()
        params = req.params
        
        search_url = f"{self.es_primary}{self.es_index}"
        
        nested_dict = lambda: defaultdict(nested_dict)
        q = nested_dict()
        
        size = 2500
        sort_direction = 'desc'
        q['query']['bool']['filter'] = []
        q['size'] = size
        
        # Filter by author
        if author is not None:
            terms = nested_dict()
            terms['terms']['author'] = [author.lower()]
            q['query']['bool']['filter'].append(terms)
        
        q['size'] = size
        q['sort']['created_utc'] = sort_direction
        
        # Add time aggregation
        q['aggs']['created_utc']['date_histogram']['field'] = 'created_utc'
        q['aggs']['created_utc']['date_histogram']['interval'] = "day"
        q['aggs']['created_utc']['date_histogram']['order']['_key'] = 'asc'
        
        # Add subreddit aggregation
        q['aggs']['subreddit']['terms']['field'] = 'subreddit.keyword'
        q['aggs']['subreddit']['terms']['size'] = size
        q['aggs']['subreddit']['terms']['order']['_count'] = 'desc'
        
        # Add link_id aggregation
        q['aggs']['link_id']['terms']['field'] = 'link_id'
        q['aggs']['link_id']['terms']['size'] = 25
        q['aggs']['link_id']['terms']['order']['_count'] = 'desc'
        
        try:
            response = requests.get(search_url, data=json.dumps(q), timeout=30)
            response.raise_for_status()
            
            es_response = json.loads(response.text)
            
            # Process link IDs (convert to base36)
            if es_response.get('aggregations', {}).get('link_id', {}).get('buckets'):
                for bucket in es_response['aggregations']['link_id']['buckets']:
                    bucket['key'] = 't3_' + base36encode(bucket['key'])
            
        except requests.RequestException as e:
            logger.error(f"Failed to analyze user {author}: {e}")
            # Try fallback
            try:
                search_url = f"{self.es_fallback}{self.es_index}"
                response = requests.get(search_url, data=json.dumps(q), timeout=30)
                response.raise_for_status()
                es_response = json.loads(response.text)
                
                if es_response.get('aggregations', {}).get('link_id', {}).get('buckets'):
                    for bucket in es_response['aggregations']['link_id']['buckets']:
                        bucket['key'] = 't3_' + base36encode(bucket['key'])
            except requests.RequestException as e2:
                logger.error(f"Both Elasticsearch nodes failed: {e2}")
                resp.status = 500
                resp.body = json.dumps({
                    "error": "Failed to analyze user",
                    "message": str(e2)
                })
                return
        
        end_time = time.time()
        
        data = {
            "data": es_response,
            "metadata": {
                "execution_time_milliseconds": round((end_time - start_time) * 1000, 2),
                "version": "v3.0"
            }
        }
        
        resp.cache_control = ['public', 'max-age=2', 's-maxage=2']
        resp.body = json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))