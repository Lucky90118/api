"""Submission search handler for Reddit API."""
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
    base36decode
)
import DBFunctions
from config import config
from logger_config import default_logger
from exceptions import ElasticsearchError, APIError

logger = default_logger


def nested_dict():
    """Create a nested defaultdict."""
    return defaultdict(nested_dict)


class SubmissionSearch:
    """Handler for submission search requests."""
    
    def __init__(self):
        """Initialize the submission search handler."""
        self.params = None
        self.es_primary, self.es_fallback = config.get_elasticsearch_urls()
        self.es_index = "/rs/submissions/_search"
    
    def on_get(self, req, resp):
        """
        Handle GET requests for submission search.
        
        Args:
            req: Falcon request object
            resp: Falcon response object
        """
        self.start_time = time.time()
        self.params = req.params
        
        try:
            if 'ids' in self.params:
                data = self.get_ids(self.params['ids'])
                end_time = time.time()
                data["metadata"] = {
                    "execution_time_milliseconds": round((end_time - self.start_time) * 1000, 2),
                    "version": "v3.0"
                }
            else:
                response = self.search(f"{self.es_primary}{self.es_index}")
                data = self._process_search_results(response)
                end_time = time.time()
                data["metadata"]["execution_time_milliseconds"] = round((end_time - self.start_time) * 1000, 2)
                data["metadata"]["version"] = "v3.0"
            
            resp.cache_control = ["public", "max-age=2", "s-maxage=2"]
            resp.body = json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))
            
        except Exception as e:
            logger.error(f"Error processing submission search: {e}", exc_info=True)
            resp.status = falcon.HTTP_500
            resp.body = json.dumps({
                "error": "Internal server error",
                "message": str(e)
            })
    
    def _process_search_results(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """Process Elasticsearch search results."""
        results = []
        data = {}
        
        # Process hits
        for hit in response["data"]["hits"]["hits"]:
            source = hit["_source"]
            source["id"] = base36encode(int(hit["_id"]))
            
            # Format subreddit_id
            if 'subreddit_id' in source and source["subreddit_id"] is not None:
                try:
                    source["subreddit_id"] = "t5_" + base36encode(source["subreddit_id"])
                except (TypeError, ValueError):
                    source["subreddit_id"] = None
            else:
                source["subreddit_id"] = None
            
            # Unescape HTML entities
            source["author_flair_text"] = html.unescape(source.get("author_flair_text", "")) or None
            source["author_flair_css_class"] = html.unescape(source.get("author_flair_css_class", "")) or None
            
            # Add full_link
            if source.get("permalink"):
                source["full_link"] = f"https://www.reddit.com{source['permalink']}"
            
            # Apply field filtering
            self._apply_field_filter(source)
            results.append(source)
        
        # Process aggregations
        if 'aggregations' in response["data"]:
            data['aggs'] = self._process_aggregations(response["data"]["aggregations"], response["data"])
        
        data['data'] = results
        data['metadata'] = {
            **response['metadata'],
            'results_returned': len(response["data"]["hits"]["hits"]),
            'timed_out': response['data']['timed_out'],
            'total_results': response['data']['hits']['total'],
            'shards': response['data']['_shards']
        }
        
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
    
    def _process_aggregations(self, aggregations: Dict[str, Any], es_response: Dict[str, Any]) -> Dict[str, Any]:
        """Process Elasticsearch aggregations."""
        aggs = {}
        
        # Subreddit aggregation
        if 'subreddit' in aggregations:
            for bucket in aggregations["subreddit"]["buckets"]:
                bucket["score"] = round(bucket["doc_count"] / bucket["bg_count"], 5)
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
        
        # Domain aggregation
        if 'domain' in aggregations:
            new_buckets = []
            for bucket in aggregations["domain"]["buckets"]:
                if 'self.' not in bucket["key"]:
                    new_buckets.append(bucket)
            aggs["domain"] = new_buckets
        
        # Time of day aggregation
        if 'time_of_day' in aggregations:
            total_bg_count = aggregations["time_of_day"]["bg_count"]
            total_doc_count = aggregations["time_of_day"]["doc_count"]
            
            for bucket in aggregations["time_of_day"]["buckets"]:
                bucket['bg_percentage'] = round(bucket['bg_count'] * 100 / total_bg_count, 5)
                bucket['doc_percentage'] = round(bucket['doc_count'] * 100 / total_doc_count, 5)
                bucket['deviation_percentage'] = round(bucket['doc_percentage'] - bucket['bg_percentage'], 4)
                bucket['utc_hour'] = bucket['key']
                bucket.pop('score', None)
                bucket.pop('key', None)
            
            aggs["time_of_day"] = sorted(
                aggregations["time_of_day"]["buckets"],
                key=lambda k: k['utc_hour']
            )
        
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
        q['query']['bool']['must_not'] = []
        
        # Process parameters
        self.params, q = process(self.params, q)
        
        # Add query parameter
        if 'q' in self.params and self.params['q'] is not None:
            sqs = nested_dict()
            sqs['simple_query_string']['query'] = self.params['q']
            sqs['simple_query_string']['default_operator'] = 'and'
            q['query']['bool']['filter'].append(sqs)
        
        # Add field-specific queries
        for field in ["title", "selftext"]:
            if field in self.params and self.params[field] is not None:
                sqs = nested_dict()
                sqs['simple_query_string']['query'] = self.params[field]
                sqs['simple_query_string']['fields'] = [field]
                sqs['simple_query_string']['default_operator'] = 'and'
                q['query']['bool']['filter'].append(sqs)
        
        # Add exclusion queries
        not_conditions = ["title:not", "q:not", "selftext:not"]
        for condition in not_conditions:
            if condition in self.params and self.params[condition] is not None:
                sqs = nested_dict()
                sqs['simple_query_string']['query'] = self.params[condition]
                if condition != 'q:not':
                    sqs['simple_query_string']['fields'] = [condition.split(":")[0]]
                sqs['simple_query_string']['default_operator'] = 'and'
                q['query']['bool']['must_not'].append(sqs)
        
        # Add aggregations
        self._add_aggregations(q)
        
        # Execute search
        response = self._execute_search(uri, q)
        
        results = {
            'data': json.loads(response.text),
            'metadata': {
                'sort': self.params['sort'],
                'sort_type': self.params['sort_type']
            }
        }
        
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
                q['aggs']['subreddit']['significant_terms']['field'] = 'subreddit.keyword'
                q['aggs']['subreddit']['significant_terms']['size'] = 1000
                q['aggs']['subreddit']['significant_terms']['script_heuristic']['script']['lang'] = 'painless'
                q['aggs']['subreddit']['significant_terms']['script_heuristic']['script']['inline'] = 'params._subset_freq'
                q['aggs']['subreddit']['significant_terms']['min_doc_count'] = min_doc_count
            
            elif agg_lower == 'author':
                q['aggs']['author']['terms']['field'] = 'author.keyword'
                q['aggs']['author']['terms']['size'] = 1000
                q['aggs']['author']['terms']['order']['_count'] = 'desc'
            
            elif agg_lower == 'created_utc':
                q['aggs']['created_utc']['date_histogram']['field'] = 'created_utc'
                if self.params['frequency'] is None:
                    self.params['frequency'] = "day"
                q['aggs']['created_utc']['date_histogram']['interval'] = self.params['frequency']
                q['aggs']['created_utc']['date_histogram']['order']['_key'] = 'asc'
            
            elif agg_lower == 'domain':
                q['aggs']['domain']['terms']['field'] = 'domain.keyword'
                q['aggs']['domain']['terms']['size'] = 1000
                q['aggs']['domain']['terms']['order']['_count'] = 'desc'
            
            elif agg_lower == 'time_of_day':
                q['aggs']['time_of_day']['significant_terms']['field'] = 'hour'
                q['aggs']['time_of_day']['significant_terms']['size'] = 25
    
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
    
    def get_ids(self, ids: Union[str, List[str]]) -> Dict[str, Any]:
        """
        Retrieve submissions by their IDs.
        
        Args:
            ids: Submission ID(s) as string or list of strings
            
        Returns:
            Dictionary containing submission data
        """
        if not isinstance(ids, (list, tuple)):
            ids = [ids]
        
        ids_to_fetch = []
        for sub_id in ids:
            sub_id = sub_id.lower()
            if sub_id.startswith("t3_"):
                sub_id = sub_id[3:]
            ids_to_fetch.append(base36decode(sub_id))
        
        q = nested_dict()
        q["query"]["terms"]["id"] = ids_to_fetch
        q["size"] = 500
        
        try:
            response = requests.get(f"{self.es_primary}/rs/submissions/_search", data=json.dumps(q), timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch submissions: {e}")
            raise ElasticsearchError(f"Failed to retrieve submissions: {e}")
        
        s = json.loads(response.text)
        results = []
        
        for hit in s.get("hits", {}).get("hits", []):
            source = hit["_source"]
            source["id"] = base36encode(int(hit["_id"]))
            
            if 'subreddit_id' in source:
                source['subreddit_id'] = "t5_" + base36encode(source['subreddit_id'])
            
            source["full_link"] = f"https://www.reddit.com{source['permalink']}"
            
            # Apply field filtering
            self._apply_field_filter(source)
            
            results.append(source)
        
        return {"data": results, "metadata": {}}


class CommentIDsGetter:
    """Handler for retrieving comment IDs for a submission."""
    
    def on_get(self, req, resp, submission_id: str):
        """
        Get comment IDs for a submission.
        
        Args:
            req: Falcon request object
            resp: Falcon response object
            submission_id: Base36 encoded submission ID
        """
        submission_id = submission_id.lower()
        if submission_id.startswith('t3_'):
            submission_id = submission_id[3:]
        
        submission_id = base36decode(submission_id)
        
        try:
            rows = DBFunctions.pgdb.execute(
                "SELECT (json->>'id')::bigint comment_id FROM comment WHERE (json->>'link_id')::int = %s ORDER BY comment_id ASC LIMIT 50000",
                submission_id
            )
        except Exception as e:
            logger.error(f"Failed to get comment IDs: {e}")
            raise APIError(f"Failed to retrieve comment IDs: {e}")
        
        results = []
        if rows:
            for row in rows:
                comment_id = row[0]
                results.append(base36encode(comment_id))
        
        data = {"data": results}
        resp.cache_control = ["public", "max-age=5", "s-maxage=5"]
        resp.body = json.dumps(data, sort_keys=True, indent=4, separators=(',', ': '))