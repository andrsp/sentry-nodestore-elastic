import base64
from datetime import datetime, timezone
import logging
import re
import zlib
from typing import Optional, List, Any
import elasticsearch
from elasticsearch import Elasticsearch
from sentry.nodestore.base import NodeStorage

logger = logging.getLogger("sentry.nodestore.elastic")


class ElasticNodeStorage(NodeStorage):
    """
    Elasticsearch backend for Sentry nodestore.
    
    This backend stores Sentry node objects in Elasticsearch instead of PostgreSQL,
    providing better scalability and performance for high-load environments.
    """
    
    logger = logger
    encoding = 'utf-8'
    
    # Index name pattern for date-based indices
    INDEX_DATE_PATTERN = re.compile(r'^sentry-(\d{4}-\d{2}-\d{2})')

    def __init__(
        self,
        es: Elasticsearch,
        index: str = 'sentry-{date}',
        refresh: bool = False,
        template_name: str = 'sentry',
        alias_name: str = 'sentry',
        validate_es: bool = False,
    ) -> None:
        """
        Initialize Elasticsearch nodestore backend.
        
        Args:
            es: Elasticsearch client instance
            index: Index name pattern with {date} placeholder (default: 'sentry-{date}')
            refresh: Whether to refresh index after writes (default: False for better performance)
            template_name: Name of the index template (default: 'sentry')
            alias_name: Name of the index alias (default: 'sentry')
            validate_es: Whether to validate Elasticsearch connection on init (default: False)
        """
        if not isinstance(es, Elasticsearch):
            raise TypeError("es parameter must be an Elasticsearch client instance")
        
        self.es = es
        self.index = index
        self.refresh = refresh
        self.template_name = template_name
        self.alias_name = alias_name
        self.validate_es = validate_es
        
        if self.validate_es:
            try:
                self.es.info()
            except Exception as e:
                raise ConnectionError(f"Failed to connect to Elasticsearch: {e}") from e

        super(ElasticNodeStorage, self).__init__()

    def bootstrap(self) -> None:
        """
        Bootstrap Elasticsearch index template.
        
        Creates an index template if it doesn't exist. Does not overwrite
        existing templates to allow manual customization.
        """
        try:
            # Do not overwrite existing template with same name
            # It may have been changed in elastic manually after creation
            # or created manually before sentry initialization
            self.es.indices.get_index_template(name=self.template_name)
            self.logger.info(
                "bootstrap.template.check",
                extra={
                    "template": self.template_name,
                    "status": "exists"
                }
            )
        except elasticsearch.exceptions.NotFoundError:
            self.logger.info(
                "bootstrap.template.check",
                extra={
                    "template": self.template_name,
                    "status": "not found"
                }
            )
            try:
                self.es.indices.put_index_template(
                    create=True,
                    name=self.template_name,
                    index_patterns=["sentry-*"],
                    template={
                        "settings": {
                            "index": {
                                "number_of_shards": 3,
                                "number_of_replicas": 0
                            }
                        },
                        "mappings": {
                            "_source": {
                                "enabled": False
                            },
                            "dynamic": "false",
                            "dynamic_templates": [],
                            "properties": {
                                "data": {
                                    "type": "text",
                                    "index": False,
                                    "store": True
                                },
                                "timestamp": {
                                    "type": "date",
                                    "store": True
                                }
                            }
                        },
                        "aliases": {
                            self.alias_name: {}
                        }
                    }
                )
                self.logger.info(
                    "bootstrap.template.create",
                    extra={
                        "template": self.template_name,
                        "alias": self.alias_name
                    }
                )
            except elasticsearch.exceptions.RequestError as e:
                self.logger.error(
                    "bootstrap.template.create.error",
                    extra={
                        "template": self.template_name,
                        "error": str(e)
                    },
                    exc_info=True
                )
                raise

    def _get_write_index(self) -> str:
        """Get the index name for writing based on current date."""
        return self.index.format(date=datetime.now(timezone.utc).strftime('%Y-%m-%d'))

    def _get_read_index(self, id: str) -> Optional[str]:
        """
        Get the index name containing the document with given ID.
        
        Optimized to use direct get through alias instead of search query.
        Falls back to search if direct get fails (for backward compatibility).
        
        Args:
            id: Document ID to find
            
        Returns:
            Index name containing the document, or None if not found
        """
        # Try direct get through alias first (more efficient)
        try:
            # Use _source: false and stored_fields to avoid loading document data
            response = self.es.get(
                id=id,
                index=self.alias_name,
                _source=False,
                stored_fields="_none_"
            )
            return response.get('_index')
        except elasticsearch.exceptions.NotFoundError:
            return None
        except elasticsearch.exceptions.RequestError:
            # Fallback to search if direct get fails (e.g., alias routing issues)
            try:
                search = self.es.search(
                    index=self.alias_name,
                    body={
                        "query": {
                            "term": {
                                "_id": id
                            }
                        },
                        "size": 1,
                        "_source": False
                    }
                )
                if search["hits"]["total"]["value"] == 1:
                    return search["hits"]["hits"][0]["_index"]
            except Exception as e:
                self.logger.warning(
                    "document.get_index.error",
                    extra={
                        "doc_id": id,
                        "error": str(e)
                    }
                )
            return None

    def _compress(self, data: bytes) -> str:
        """
        Compress and encode data for storage.
        
        Args:
            data: Raw bytes to compress
            
        Returns:
            Base64-encoded compressed string
        """
        if not isinstance(data, bytes):
            raise TypeError(f"data must be bytes, got {type(data)}")
        return base64.b64encode(zlib.compress(data)).decode(self.encoding)

    def _decompress(self, data: str) -> bytes:
        """
        Decompress and decode data from storage.
        
        Args:
            data: Base64-encoded compressed string
            
        Returns:
            Decompressed bytes
        """
        if not isinstance(data, str):
            raise TypeError(f"data must be str, got {type(data)}")
        try:
            return zlib.decompress(base64.b64decode(data))
        except (ValueError, zlib.error) as e:
            raise ValueError(f"Failed to decompress data: {e}") from e

    def delete(self, id: str) -> None:
        """
        Delete a node by ID.
        
        Args:
            id: Document ID to delete
            
        Example:
            >>> nodestore.delete('key1')
        """
        if not id:
            raise ValueError("id cannot be empty")
        
        try:
            # Use direct delete instead of delete_by_query for better performance
            index = self._get_read_index(id)
            if index:
                self.es.delete(id=id, index=index, refresh=self.refresh)
            else:
                # Fallback to delete_by_query if index not found
                self.es.delete_by_query(
                    index=self.alias_name,
                    query={
                        "term": {
                            "_id": id
                        }
                    }
                )
            self.logger.info(
                "document.delete.executed",
                extra={
                    "doc_id": id
                }
            )
        except elasticsearch.exceptions.NotFoundError:
            # Document doesn't exist, which is fine
            pass
        except elasticsearch.exceptions.ConflictError:
            # Concurrent deletion, which is fine
            pass
        except Exception as e:
            self.logger.error(
                "document.delete.error",
                extra={
                    "doc_id": id,
                    "error": str(e)
                },
                exc_info=True
            )
            raise

    def delete_multi(self, id_list: List[str]) -> None:
        """
        Delete multiple nodes.
        
        Note: This is not guaranteed to be atomic and may result in a partial
        delete.
        
        Args:
            id_list: List of document IDs to delete
            
        Example:
            >>> delete_multi(['key1', 'key2'])
        """
        if not id_list:
            return
        
        if not isinstance(id_list, list):
            raise TypeError(f"id_list must be a list, got {type(id_list)}")
        
        try:
            response = self.es.delete_by_query(
                index=self.alias_name,
                query={
                    "ids": {
                        "values": id_list
                    }
                },
                refresh=self.refresh
            )
            self.logger.info(
                "document.delete_multi.executed",
                extra={
                    "docs_to_delete": len(id_list),
                    "docs_deleted": response.get("deleted", 0)
                }
            )
        except elasticsearch.exceptions.NotFoundError:
            # Indices don't exist, which is fine
            pass
        except elasticsearch.exceptions.ConflictError:
            # Concurrent deletion, which is fine
            pass
        except Exception as e:
            self.logger.error(
                "document.delete_multi.error",
                extra={
                    "docs_to_delete": len(id_list),
                    "error": str(e)
                },
                exc_info=True
            )
            raise


    def _get_bytes(self, id: str) -> Optional[bytes]:
        """
        Get raw bytes for a node by ID.
        
        Args:
            id: Document ID to retrieve
            
        Returns:
            Decompressed bytes, or None if not found
            
        Example:
            >>> nodestore._get_bytes('key1')
            b'{"message": "hello world"}'
        """
        if not id:
            return None
        
        index = self._get_read_index(id)

        if index is not None:
            try:
                response = self.es.get(id=id, index=index, stored_fields=["data"])
                if 'fields' in response and 'data' in response['fields']:
                    return self._decompress(response['fields']['data'][0])
                else:
                    self.logger.warning(
                        "document.get.warning",
                        extra={
                            "doc_id": id,
                            "index": index,
                            "error": "data field not found in response"
                        }
                    )
                    return None
            except elasticsearch.exceptions.NotFoundError:
                return None
            except Exception as e:
                self.logger.error(
                    "document.get.error",
                    extra={
                        "doc_id": id,
                        "index": index,
                        "error": str(e)
                    },
                    exc_info=True
                )
                return None
        else:
            self.logger.warning(
                "document.get.warning",
                extra={
                    "doc_id": id,
                    "error": "index containing doc_id not found"
                }
            )
            return None


    def _set_bytes(self, id: str, data: bytes, ttl: Optional[int] = None) -> None:
        """
        Set raw bytes for a node by ID.
        
        Args:
            id: Document ID
            data: Raw bytes to store
            ttl: Time to live in seconds (not currently used, reserved for future use)
            
        Example:
            >>> nodestore._set_bytes('key1', b"{'foo': 'bar'}")
        """
        if not id:
            raise ValueError("id cannot be empty")
        
        if not isinstance(data, bytes):
            raise TypeError(f"data must be bytes, got {type(data)}")
        
        index = self._get_write_index()
        try:
            self.es.index(
                id=id,
                index=index,
                document={
                    'data': self._compress(data),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                },
                refresh=self.refresh,
            )
        except Exception as e:
            self.logger.error(
                "document.set.error",
                extra={
                    "doc_id": id,
                    "index": index,
                    "error": str(e)
                },
                exc_info=True
            )
            raise

    def cleanup(self, cutoff: datetime) -> None:
        """
        Clean up indices older than the cutoff date.
        
        Args:
            cutoff: Datetime threshold - indices older than this will be deleted
        """
        if not isinstance(cutoff, datetime):
            raise TypeError(f"cutoff must be a datetime object, got {type(cutoff)}")
        
        # Ensure cutoff is timezone-aware
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        
        try:
            alias_indices = self.es.indices.get_alias(index=self.alias_name)
        except elasticsearch.exceptions.NotFoundError:
            self.logger.warning(
                "cleanup.alias.not_found",
                extra={
                    "alias": self.alias_name
                }
            )
            return
        
        deleted_count = 0
        skipped_count = 0
        
        for index in alias_indices:
            # Parse date from index name using regex for more robust parsing
            # Handles indices with postfixes like '-fixed' or '-reindex'
            match = self.INDEX_DATE_PATTERN.match(index)
            if not match:
                self.logger.warning(
                    "cleanup.index.skip",
                    extra={
                        "index": index,
                        "reason": "index name does not match expected pattern"
                    }
                )
                skipped_count += 1
                continue
            
            try:
                index_date_str = match.group(1)
                index_ts = datetime.strptime(index_date_str, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                
                if index_ts < cutoff:
                    try:
                        self.es.indices.delete(index=index)
                        deleted_count += 1
                        self.logger.info(
                            "index.delete.executed",
                            extra={
                                "index": index,
                                "index_ts": index_ts.timestamp(),
                                "cutoff_ts": cutoff.timestamp(),
                                "status": "deleted"
                            }
                        )
                    except elasticsearch.exceptions.NotFoundError:
                        self.logger.info(
                            "index.delete.error",
                            extra={
                                "index": index,
                                "error": "not found"
                            }
                        )
                    except Exception as e:
                        self.logger.error(
                            "index.delete.error",
                            extra={
                                "index": index,
                                "error": str(e)
                            },
                            exc_info=True
                        )
            except ValueError as e:
                self.logger.warning(
                    "cleanup.index.skip",
                    extra={
                        "index": index,
                        "reason": f"failed to parse date: {e}"
                    }
                )
                skipped_count += 1
        
        self.logger.info(
            "cleanup.completed",
            extra={
                "cutoff_ts": cutoff.timestamp(),
                "deleted_count": deleted_count,
                "skipped_count": skipped_count,
                "total_checked": len(alias_indices)
            }
        )
