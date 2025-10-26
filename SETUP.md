# Pushshift Reddit API - Setup Guide

## Installation

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database

Copy the example configuration file and update with your credentials:

```bash
cp credentials.ini.example credentials.ini
```

Edit `credentials.ini` with your database credentials:

```ini
[database]
host = jupiter
port = 5432
database = reddit
user = your_username
password = your_password

[elasticsearch]
primary = http://mars:9200
fallback = http://jupiter:9200
```

Alternatively, you can use environment variables:

```bash
export DB_HOST=jupiter
export DB_PORT=5432
export DB_NAME=reddit
export DB_USER=your_username
export DB_PASSWORD=your_password
export ES_PRIMARY=http://mars:9200
export ES_FALLBACK=http://jupiter:9200
```

### 3. Run the API

```bash
python api/api.py
```

Or using Gunicorn (recommended for production):

```bash
gunicorn api.api:api
```

## API Endpoints

### Comment Search
```
GET /reddit/search/comment?q=search_term&subreddit=subreddit_name&size=100
```

### Submission Search
```
GET /reddit/search/submission?q=search_term&size=100
```

### User Analysis
```
GET /reddit/analyze/user/username
```

### Get Comment IDs for Submission
```
GET /reddit/submission/comment_ids/{submission_id}
```

## Query Parameters

### Common Parameters
- `q` - Search query
- `size` - Number of results (max 500, default 25)
- `sort` - Sort order: `asc` or `desc`
- `sort_type` - Sort by: `created_utc`, `score`, `num_comments`
- `after` - Return results after this time (epoch or relative: `30d`, `24h`)
- `before` - Return results before this time
- `fields` - Return specific fields only (comma-separated)
- `subreddit` - Filter by subreddit(s)
- `author` - Filter by author(s)
- `aggs` - Request aggregations (comma-separated)

### Time Formats
- Epoch timestamp: `1609459200`
- Relative: `30d` (30 days), `24h` (24 hours), `60m` (60 minutes)

### Aggregations
- `subreddit` - Top subreddits
- `author` - Top authors
- `created_utc` - Time-based histogram
- `link_id` - Top submissions (comments only)
- `domain` - Top domains (submissions only)

## Examples

### Search for Python-related comments in the past 7 days
```bash
curl "http://localhost:8000/reddit/search/comment?q=python&after=7d&size=50"
```

### Get top subreddits discussing AI
```bash
curl "http://localhost:8000/reddit/search/comment?q=artificial+intelligence&aggs=subreddit&size=0"
```

### Analyze a user's activity
```bash
curl "http://localhost:8000/reddit/analyze/user/SomeUser"
```

## Architecture

The API uses:
- **Falcon** for the web framework
- **Elasticsearch** for primary data storage and search
- **PostgreSQL** as a fallback data store
- **Logging** for monitoring and debugging

## Logging

Logs are written to `api.log` with rotation (max 10MB, 5 backups).
Error-level logs are also printed to console.

## Error Handling

The API includes:
- Automatic Elasticsearch failover
- Database connection retry logic
- Proper error responses with status codes
- Comprehensive logging

## Development

### Code Structure
- `api/api.py` - Main entry point
- `Comment.py` - Comment search handlers
- `Submission.py` - Submission search handlers
- `User.py` - User analysis
- `Parameters.py` - Query parameter processing
- `Helpers.py` - Utility functions
- `DBFunctions.py` - Database access
- `config.py` - Configuration management
- `logger_config.py` - Logging setup
- `exceptions.py` - Custom exceptions
