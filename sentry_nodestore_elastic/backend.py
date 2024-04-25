import base64
from datetime import datetime, timezone
import logging
import zlib
import elasticsearch
from sentry.nodestore.base import NodeStorage

class ElasticNodeStorage(NodeStorage):
    logger = logging.getLogger("sentry.nodestore.elastic")

    encoding = 'utf-8'

    def __init__(
        self,
        es,
        index='sentry-{date}',
        refresh=False,
        template_name='sentry',
        alias_name='sentry',
        validate_es=False,
    ):
        self.es = es
        self.index = index
        self.refresh = refresh
        self.template_name = template_name
        self.alias_name = alias_name
        self.validate_es = validate_es

        super(ElasticNodeStorage, self).__init__()

    def bootstrap(self):
        try:
            # do not owerwrite existing template with same name
            # it may have been changed in elastic manually after creation
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
            self.es.indices.put_index_template(
                create = True,
                name = self.template_name,
                index_patterns = [
                    "sentry-*"
                ],
                template = {
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

    def _get_write_index(self):
        return self.index.format(date=datetime.today().strftime('%Y-%m-%d'))

    def _get_read_index(self, id):
        search = self.es.search(
            index=self.alias_name,
            body={
                "query": {
                    "term": {
                        "_id": id
                    },
                },
            }
        )
        if search["hits"]["total"]["value"] == 1:
            return search["hits"]["hits"][0]["_index"]
        else:
            return None

    def _compress(self, data):
        return base64.b64encode(zlib.compress(data)).decode(self.encoding)

    def _decompress(self, data):
        return zlib.decompress(base64.b64decode(data))

    def delete(self, id):
        """
        >>> nodestore.delete('key1')
        """

        try:
            self.logger.info(
                "document.delete.executed",
                extra={
                    "doc_id": id
                }
            )
            self.es.delete_by_query(
                index=self.alias_name,
                query = {
                    "term": {
                      "_id": id
                    }
                }
            )
        except elasticsearch.exceptions.NotFoundError:
            pass
        except elasticsearch.exceptions.ConflictError:
            pass

    def delete_multi(self, id_list):
        """
        Delete multiple nodes.
        Note: This is not guaranteed to be atomic and may result in a partial
        delete.
        >>> delete_multi(['key1', 'key2'])
        """

        try:
            response = self.es.delete_by_query(
                index=self.alias_name,
                query = {
                    "ids": {
                        "values": id_list
                    }
                }
            )
            self.logger.info(
                "document.delete_multi.executed",
                extra={
                    "docs_to_delete": len(id_list),
                    "docs_deleted": response["deleted"]
                }
            )
        except elasticsearch.exceptions.NotFoundError:
            pass
        except elasticsearch.exceptions.ConflictError:
            pass


    def _get_bytes(self, id):
        """
        >>> nodestore._get_bytes('key1')
        b'{"message": "hello world"}'
        """
        index = self._get_read_index(id)

        if index is not None:
            try:
                response = self.es.get(id=id, index=index, stored_fields=["data"])
            except elasticsearch.exceptions.NotFoundError:
                return None
            else:
                return self._decompress(response['fields']['data'][0])
        else:
            self.logger.warning(
                "document.get.warning",
                extra={
                    "doc_id": id,
                    "error": "index containing doc_id not found"
                }
            )
            return None


    def _set_bytes(self, id, data, ttl=None):
        """
        >>> nodestore.set('key1', b"{'foo': 'bar'}")
        """
        index = self._get_write_index()
        self.es.index(
            id=id,
            index=index,
            document={'data': self._compress(data), 'timestamp': datetime.utcnow().isoformat()},
            refresh=self.refresh,
        )

    def cleanup(self, cutoff: datetime):
        for index in self.es.indices.get_alias(index=self.alias_name):
            # parse date from manually changed indices after reindex
            # (they may have postfixes like '-fixed' or '-reindex')
            index_date = '-'.join(index.split('-')[1:4])
            index_ts = datetime.strptime(index_date, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
            if index_ts < cutoff:
                try:
                    self.es.indices.delete(index=index)
                except elasticsearch.exceptions.NotFoundError:
                    self.logger.info(
                        "index.delete.error",
                        extra={
                            "index": index,
                            "error": "not found"
                        }
                    )
                else:
                    self.logger.info(
                        "index.delete.executed",
                        extra={
                            "index": index,
                            "index_ts": index_ts.timestamp(),
                            "cutoff_ts": cutoff.timestamp(),
                            "status": "deleted"
                        }
                    )
