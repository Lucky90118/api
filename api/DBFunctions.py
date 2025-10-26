"""Database access layer with connection pooling and retry logic."""
import psycopg2
import time
import logging
from psycopg2 import OperationalError, DatabaseError
from config import config

logger = logging.getLogger(__name__)


class PostgreSQLManager:
    """Manages PostgreSQL connections with retry logic and error handling."""

    def __init__(self):
        """Initialize the database manager."""
        self.db = None
        self.max_retries = 5
        self.retry_delay = 1
        self.connect()

    def connect(self):
        """Establish connection to PostgreSQL database."""
        try:
            connection_string = config.get_db_connection_string()
            self.db = psycopg2.connect(connection_string)
            self.db.set_session(autocommit=True)
            logger.info(f"Successfully connected to PostgreSQL database on {config.db_config['host']}")
        except (OperationalError, psycopg2.OperationalError) as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def execute(self, sql: str, params: tuple = None):
        """
        Execute a SQL query with retry logic.
        
        Args:
            sql: SQL query string
            params: Query parameters as a tuple
            
        Returns:
            Query results as a list of tuples
            
        Raises:
            DatabaseError: If query fails after all retries
        """
        retries = self.max_retries
        
        while retries > 0:
            try:
                cur = self.db.cursor()
                
                # Execute with parameter binding for SQL injection prevention
                if params is not None:
                    cur.execute(sql, (params,))
                else:
                    cur.execute(sql)
                
                rows = cur.fetchall()
                cur.close()
                
                logger.debug(f"Query executed successfully: {sql[:50]}...")
                return rows
                
            except (OperationalError, psycopg2.OperationalError) as e:
                logger.warning(f"Database operation failed, retrying... ({retries} retries left): {e}")
                retries -= 1
                
                if retries <= 0:
                    logger.error(f"Database operation failed after {self.max_retries} retries")
                    raise DatabaseError(f"Failed to execute query after {self.max_retries} retries: {e}")
                
                time.sleep(self.retry_delay)
                self.connect()
                
            except DatabaseError as e:
                logger.error(f"Database error: {e}")
                raise
                
            except Exception as e:
                logger.error(f"Unexpected error executing query: {e}")
                raise

    def health_check(self) -> bool:
        """
        Check database connection health.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            cur = self.db.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            cur.close()
            return True
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False


# Global database instance
pgdb = PostgreSQLManager()