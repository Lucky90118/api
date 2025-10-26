"""Comment search handler for Reddit API."""
import time
import html
import json
import requests
import falcon
from collections import defaultdict
from typing import Dict, Any, List, Union
import logging

from Parameters import process
from Helpers import (
    base36encode,
    base36decode,
    get_submissions_from_es
)
import DBFunctions
from config import config
from logger_config import default_logger
from exceptions import ElasticsearchError, APIError

logger = default_logger


def nested_dict():
    """Create a nested defaultdict."""
    return defaultdict(nested_dict)


class CommentSearch:
    """Handler for comment search requests."""
    
    def __init__(self):
        """Initialize the comment search handler."""
        self.params = None
        self.es_primary, self.es_fallback = config.get_elasticsearch_urls()
        self.es_index = "/rc/comments/_search"
    
    def on_get(self, req, resp):
        """
        Handle GET requests for comment search.
        
        Args:
            req: Falcon request object
            resp: Falcon response object
        """
        start_time = time.time()
        self.params = req.params
        
        try:
            if 'ids' in self.params:
                data = self.get_ids(self.params['ids'])
            else:
                data = self.do_elasticsearch()
            
            end_time = time.time()
            data["metadata"]["execution_time_milliseconds"] = round((end_time - start_time) * 1000, 2)
            data["metadata"]["version"] = "v3.0"
            
            resp.cache_control = ["public", "max-age=2", "s-maxage=2"]
            resp.body = json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))
            
        except Exception as e:
            logger.error(f"Error processing comment search: {e}", exc_info=True)
            resp.status = falcon.HTTP_500
            resp.body = json.dumps({
                "error": "Internal server error",
                "message": str(e)
            })
    
    def get_ids(self, ids: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Retrieve comments by their IDs from PostgreSQL.
        
        Args:
            ids: Comment ID(s) as string or list of strings
            
        Returns:
            Dictionary containing comment data and metadata
        """
        if not isinstance(ids, (list, tuple)):
            ids = [ids]
        
        ids_to_get_from_db = []
        for comment_id in ids:
            comment_id = comment_id.lower()
            if comment_id.startswith("t1_"):
                comment_id = comment_id[3:]
            ids_to_get_from_db.append(base36decode(comment_id))
        
        try:
            rows = DBFunctions.pgdb.execute(
                "SELECT * FROM comment WHERE (json->>'id')::bigint IN %s LIMIT 5000",
                tuple(ids_to_get_from_db)
            )
        except Exception as e:
            logger.error(f"Error fetching comments from PostgreSQL: {e}")
            raise APIError(f"Failed to retrieve comments: {e}")
        
        results = []
        for row in rows:
            comment = row[0]
            comment['id'] = base36encode(comment['id'])
            
            # Format parent_id and link_id
            if 'parent_id' not in comment or comment['parent_id'] is None:
                comment['parent_id'] = "t3_" + base36encode(comment['link_id'])
            elif comment['parent_id'] == comment['link_id']:
                comment['parent_id'] = "t3_" + base36encode(comment['link_id'])
            else:
                comment['parent_id'] = "t1_" + base36encode(comment['parent_id'])
            
            # Format subreddit_id
            if 'subreddit_id' in comment:
                comment['subreddit_id'] = "t5_" + base36encode(comment['subreddit_id'])
            
            comment['link_id'] = "t3_" + base36encode(comment['link_id'])
            comment.pop('name', None)
            results.append(comment)
        
        return {"data": results, "metadata": {}}
    
    def do_elasticsearch(self) -> Dict[str, Any]:
        """
        Perform Elasticsearch search for comments.
        
        Returns:
            Dictionary containing search results and aggregations
        """
        response = self.search(f"{self.es_primary}{self.es_index}")
        results = []
        data = {}
        
        # Process search results
        for hit in response["data"]["hits"]["hits"]:
            source = hit["_source"]
            source["id"] = base36encode(int(hit["_id"]))
            source["link_id"] = "t3_" + base36encode(source["link_id"])
            
            # Format parent_id
            if 'parent_id' in source:
                source["parent_id"] = "t1_" + base36encode(source["parent_id"])
            else:
                source["parent_id"] = source["link_id"]
            
            source["subreddit_id"] = "t5_" + base36encode(source["subreddit_id"])
            
            # Unescape HTML entities
            source["author_flair_text"] = html.unescape(source.get("author_flair_text", "")) or None
            source["author_flair_css_class"] = html.unescape(source.get("author_flair_css_class", "")) or None
            
            # Apply field filtering if requested
            self._apply_field_filter(source)
            
            results.append(source)
        
        # Process aggregations
        if 'aggregations' in response["data"]:
            data["aggs"] = self._process_aggregations(response["data"]["aggregations"])
        
        data["data"] = results
        data["metadata"] = {}
        data["metadata"] = response["metadata"]
        data["metadata"]["results_returned"] = len(response["data"]["hits"]["hits"])
        data["metadata"]["timed_out"] = response["data"]["timed_out"]
        data["metadata"]["total_results"] = response["data"]["hits"]["total"]
        data["metadata"]["shards"] = response["data"]["_shards"]
        
        return data
    
    def _apply_field_filter(self, source: Dict[str, Any]):
        """Apply field filtering if 'fields' parameter is specified."""
        if 'fields' in self.params and self.params['fields'] is not None:
            if isinstance(self.params['fields'], str):
                self.params['fields'] = [self.params['fields']]
            self.params['fields'] = [x.lower() for x in self.params['fields']]
            
            for key in list(source.keys()):
                if key.lower() not in self.params['fields']:
                    source.pop(key, None)
    
    def _process_aggregations(self, aggregations: Dict[str, Any]) -> Dict[str, Any]:
        """Process Elasticsearch aggregations."""
        aggs = {}
        
        # Subreddit aggregation
        if 'subreddit' in aggregations:
            for bucket in aggregations["subreddit"]["buckets"]:
                bucket["score"] = bucket["doc_count"] / bucket["bg_count"]
            aggs["subreddit"] = sorted(
                aggregations["subreddit"]["buckets"],
                key=lambda k: k['score'],
                reverse=True
            )
        
        # Author aggregation
        if 'author' in aggregations:
            for bucket in aggregations["author"]["buckets"]:
                if 'score' in bucket:
                    bucket["score"] = bucket["doc_count"] / bucket["bg_count"]
            aggs["author"] = aggregations["author"]["buckets"]
        
        # Time aggregation
        if 'created_utc' in aggregations:
            for bucket in aggregations["created_utc"]["buckets"]:
                bucket.pop('key_as_string', None)
                bucket["key"] = int(bucket["key"] / 1000)
            aggs["created_utc"] = aggregations["created_utc"]["buckets"]
        
        # Link ID aggregation
        if 'link_id' in aggregations:
            ids = []
            for bucket in aggregations["link_id"]["buckets"]:
                if 'score' in bucket:
                    bucket["score"] = bucket["doc_count"] / bucket["bg_count"]
                ids.append(bucket["key"])
            
            submission_data = get_submissions_from_es(ids)
            
            after = 0
            if "after" in self.params:
                after = int(self.params["after"])
            
            newlist = []
            for item in aggregations["link_id"]["buckets"]:
                if item["key"] in submission_data and submission_data[item["key"]]["created_utc"] > after:
                    item["data"] = submission_data[item["key"]]
                    item["data"]["full_link"] = f"https://www.reddit.com{item['data']['permalink']}"
                    newlist.append(item)
            
            aggs["link_id"] = newlist
        
        return aggs
    
    def search(self, uri: str) -> Dict[str, Any]:
        """
        Build and execute Elasticsearch query.
        
        Args:
            uri: Elasticsearch endpoint URL
            
        Returns:
            Elasticsearch response data
        """
        q = nested_dict()
        q['query']['bool']['filter'] = []
        
        # Add query parameter if present
        if 'q' in self.params and self.params['q'] is not None:
            sqs = nested_dict()
            sqs['simple_query_string']['query'] = self.params['q']
            sqs['simple_query_string']['fields'] = ['body']
            sqs['simple_query_string']['default_operator'] = 'and'
            q['query']['bool']['filter'].append(sqs)
        
        # Process parameters
        self.params, q = process(self.params, q)
        
        # Add aggregations
        self._add_aggregations(q)
        
        # Execute search with failover
        response = self._execute_search(uri, q)
        
        results = {
            'data': json.loads(response.text),
            'metadata': {
                'size': self.params['size'],
                'sort': self.params['sort'],
                'sort_type': self.params['sort_type']
            }
        }
        
        if 'after' in self.params and self.params['after'] is not None:
            results['metadata']['after'] = self.params['after']
        
        return results
    
    def _add_aggregations(self, q: defaultdict):
        """Add aggregation clauses to the Elasticsearch query."""
        if 'aggs' not in self.params or not self.params['aggs']:
            return
        
        if isinstance(self.params['aggs'], str):
            self.params['aggs'] = [self.params['aggs']]
        
        min_doc_count = 0
        if 'min_doc_count' in self.params and self.params['min_doc_count'] is not None:
            from Helpers import looks_like_int
            if looks_like_int(self.params['min_doc_count']):
                min_doc_count = self.params['min_doc_count']
        
        for agg in self.params['aggs']:
            agg_lower = agg.lower()
            
            if agg_lower == 'subreddit':
                q['aggs']['subreddit']['significant_terms']['field'] = "subreddit.keyword"
                q['aggs']['subreddit']['significant_terms']['size'] = 1000
                q['aggs']['subreddit']['significant_terms']['script_heuristic']['script']['lang'] = "painless"
                q['aggs']['subreddit']['significant_terms']['script_heuristic']['script']['inline'] = "params._subset_freq"
                q['aggs']['subreddit']['significant_terms']['min_doc_count'] = min_doc_count
            
            elif agg_lower == 'author':
                q['aggs']['author']['terms']['field'] = 'author.keyword'
                q['aggs']['author']['terms']['size'] = 1000
                q['aggs']['author']['terms']['order']['_count'] = 'desc'
            
            elif agg_lower == 'created_utc':
                q['aggs']['created_utc']['date_histogram']['field'] = "created_utc"
                if self.params['frequency'] is None:
                    self.params['frequency'] = "day"
                q['aggs']['created_utc']['date_histogram']['interval'] = self.params['frequency']
                q['aggs']['created_utc']['date_histogram']['order']['_key'] = "asc"
            
            elif agg_lower == 'link_id':
                q['aggs']['link_id']['terms']['field'] = "link_id"
                q['aggs']['link_id']['terms']['size'] = 250
                q['aggs']['link_id']['terms']['order']['_count'] = "desc"
    
    def _execute_search(self, uri: str, q: defaultdict) -> requests.Response:
        """Execute Elasticsearch search with failover."""
        try:
            response = requests.get(uri, data=json.dumps(q), timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.warning(f"Elasticsearch primary failed: {e}, trying fallback...")
            try:
                fallback_uri = f"{self.es_fallback}{self.es_index}"
                response = requests.get(fallback_uri, data=json.dumps(q), timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as e2:
                logger.error(f"Both Elasticsearch nodes failed: {e2}")
                raise ElasticsearchError(f"Failed to connect to Elasticsearch: {e2}")