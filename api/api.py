#!/usr/bin/env python3
"""Main API entry point for Pushshift Reddit API."""
import falcon
from Comment import CommentSearch
from Submission import SubmissionSearch, CommentIDsGetter
from User import UserAnalyzer
from logger_config import default_logger

logger = default_logger
api = falcon.API()

api.add_route('/reddit/search', CommentSearch())
api.add_route('/reddit/comment/search', CommentSearch())
api.add_route('/reddit/search/comment', CommentSearch())
api.add_route('/reddit/search/submission', SubmissionSearch())
api.add_route('/reddit/submission/search', SubmissionSearch())
api.add_route('/reddit/analyze/user/{author}', UserAnalyzer())
api.add_route('/get/comment_ids/{submission_id}', CommentIDsGetter())
api.add_route('/reddit/submission/comment_ids/{submission_id}', CommentIDsGetter())


if __name__ == '__main__':
    from wsgiref import simple_server
    httpd = simple_server.make_server('127.0.0.1', 8000, api)
    logger.info("Serving on http://127.0.0.1:8000")
    httpd.serve_forever()