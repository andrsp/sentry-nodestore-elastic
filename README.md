# sentry-nodestore-elastic

Sentry nodestore Elasticsearch backend

[![image](https://img.shields.io/pypi/v/sentry-nodestore-elastic.svg)](https://pypi.python.org/pypi/sentry-nodestore-elastic)

Supported Sentry 24.x & elasticsearch 8.x versions

Use Elasticsearch cluster for store node objects from Sentry
By default selfhosted Sentry uses Postgresql database for settings and nodestore, and under high load it becomes a bottleneck, database size growing fast and slowing down entire system
Switching nodestore to dedicated Elasticsearch cluster provides more scalability:
- Elasticsearch cluster may be scaled horizontally by adding more data nodes (Postgres not)
- Data in Elasticsearch may be sharded and replicated between data nodes, which increases throughput
- Elasticsearch can rebalance automatically when new data nodes added
- Scheduled Sentry cleanup performs much faster and stable when using elastic nodestore because of simple deleting old indices (cleanup in Postgresql terabyte-size nodestore is a huge pain)

## Installation

Rebuild sentry docker image with nodestore package installation

``` shell
FROM getsentry/sentry:24.4.1
RUN  pip install sentry-nodestore-elastic
```

## Configuration

Set `SENTRY_NODESTORE` at your `sentry.conf.py`

``` python
from elasticsearch import Elasticsearch
es = Elasticsearch(
        ['https://username:password@elasticsearch:9200'],
        http_compress=True,
        request_timeout=60,
        max_retries=3,
        retry_on_timeout=True,
        # ❯ openssl s_client -connect elasticsearch:9200 < /dev/null 2>/dev/null | openssl x509 -fingerprint -noout -in /dev/stdin
        ssl_assert_fingerprint=(
            "PUT_FINGERPRINT_HERE"
        )
    )
SENTRY_NODESTORE = 'sentry_nodestore_elastic.ElasticNodeStorage'
SENTRY_NODESTORE_OPTIONS = {
    'es': es,
    'refresh': False,  # ref: https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html
    # other ES related options
}

from sentry.conf.server import *  # default for sentry.conf.py
INSTALLED_APPS = list(INSTALLED_APPS)
INSTALLED_APPS.append('sentry-nodestore-elastic')
INSTALLED_APPS = tuple(INSTALLED_APPS)
```

## Usage

### Setup elasticsearch index template

Elasticsearch shoud be up and running before this step, this will create index template in elasticsearch

``` shell
sentry upgrade --with-nodestore
```

Or you can prepare index template manually with this json, it may be customized for your needs (but template name should be `sentry` because of nodestore init script checks)

``` json
{
  "template": {
    "settings": {
      "index": {
        "number_of_shards": "3",
        "number_of_replicas": "0",
        "routing": {
          "allocation": {
            "include": {
              "_tier_preference": "data_content"
            }
          }
        }
      }
    },
    "mappings": {
      "dynamic": "false",
      "dynamic_templates": [],
      "properties": {
        "data": {
          "type": "text",
          "index": false,
          "store": true
        },
        "timestamp": {
          "type": "date",
          "store": true
        }
      }
    },
    "aliases": {
      "sentry": {}
    }
  }
}
```

### Migrate data from default Postgres nodestore to elasticsearch

    Postgres and Elasticsearch must be accessible from place where you run this code

``` python
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError
import psycopg2

es = Elasticsearch(
        ['https://username:password@elasticsearch:9200'],
        http_compress=True,
        request_timeout=60,
        max_retries=3,
        retry_on_timeout=True,
        # ❯ openssl s_client -connect elasticsearch:9200 < /dev/null 2>/dev/null | openssl x509 -fingerprint -noout -in /dev/stdin
        ssl_assert_fingerprint=(
            "PUT_FINGERPRINT_HERE"
        )
    )

name = 'sentry'

conn = psycopg2.connect(dbname="sentry", user="sentry", password="password", host="hostname", port="5432")

cur = conn.cursor()
cur.execute("SELECT reltuples AS estimate FROM pg_class where relname = 'nodestore_node'")
result = cur.fetchone()
count = int(result[0])
print(f"Estimated rows: {count}")
cur.close()

cursor = conn.cursor(name='fetch_nodes')
cursor.execute("SELECT * FROM nodestore_node ORDER BY timestamp ASC")

while True:
    records = cursor.fetchmany(size=2000)

    if not records:
        break

    bulk_data = []

    for r in records:
        id = r[0]
        data = r[1]
        date = r[2].strftime("%Y-%m-%d")
        ts = r[2].isoformat()
        index = f"sentry-{date}"

        doc = {
            'data': data,
            'timestamp' : ts
        }

        action = {
                "_index": index,
                "_id": id,
                "_source": doc
        }

        bulk_data.append(action)

    bulk(es, bulk_data)
    count = count - 2000
    print(f"Remainig rows: {count}")

cursor.close()
conn.close()
```
