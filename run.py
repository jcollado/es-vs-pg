#!/usr/bin/env python
"""Full text search comparison script.

The script inserts a number of rows in both Elasticsearch and PostgreSQL and
compares the performance of a full text search query.

"""

import argparse
import logging
import sys

from pprint import pformat

from contexttimer import Timer
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from faker import Factory
from six.moves import range
from sqlalchemy import (
    Column,
    Index,
    MetaData,
    Table,
    create_engine,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.types import (
    TEXT,
    TIMESTAMP,
)
from sqlalchemy_utils import (
    create_database,
    database_exists,
)


def main(argv=None):
    """Compare Elasticsearch vs PostgreSQL full text search performance."""
    if argv is None:
        argv = sys.argv[1:]

    args = parse_arguments(argv)
    configure_logging(args.log_level)

    records, queries = generate_random_data(
        args.record_count, args.query_count)
    es_index_timer, es_query_timer = (
        elasticsearch(args.elasticsearch, records, queries)
    )
    pg_insert_timer, pg_query_timer = (
        postgresql(args.postgresql, records, queries)
    )
    logging.info(
        'Summary:\n'
        'Elasticsearch:\n'
        '- Index:  %f\n'
        '- Query:\n'
        '  - Per query: %f\n'
        '  - Total: %f\n'
        'PostgreSQL:\n'
        '- Insert: %f\n'
        '- Query:\n'
        '  - Per query: %f\n'
        '  - Total: %f\n',
        es_index_timer.elapsed,
        es_query_timer.elapsed / args.query_count,
        es_query_timer.elapsed,
        pg_insert_timer.elapsed,
        pg_query_timer.elapsed / args.query_count,
        pg_query_timer.elapsed,
    )


def generate_random_data(record_count, query_count):
    """Generate records/queries random data.

    :param record_count: Number of random records to generate
    :type record_count: int
    :param query_count: Number of random queries to generate
    :type query_count: int
    :returns: Random records and queries
    :type: tuple(list(dict(str)), list(str))

    """
    words_per_query = 3

    fake = Factory.create()
    logging.debug('Generating %d random log records...', record_count)
    with Timer() as records_timer:
        records = [
            {
                'timestamp': fake.iso8601(),
                'message': fake.text()
            }
            for _ in range(record_count)
        ]
    logging.debug(
        'Generating log records took %f seconds', records_timer.elapsed)

    logging.debug('Generating %d query arguments...', query_count)
    with Timer() as queries_timer:
        queries = [
            ' '.join(fake.words(nb=words_per_query))
            for _ in range(query_count)
        ]
    logging.debug(
        'Generating query arguments took %f seconds', queries_timer.elapsed)

    return records, queries


def elasticsearch(host, documents, queries):
    """Index documents and run search queries in elasticsarch.

    :param host: Elasticsearch server location
    :type count: str
    :param documents: Documents to be inserted
    :type documents: list(dict(str))
    :type queries: Queries to execute
    :param queries: list(str)
    :returns: Insert and query timers
    :rtype: tuple(contexttimer.Timer, contexttimer.Timer)

    """
    index_name = 'index'
    document_type = 'log'

    logging.debug('Connecting to elasticsearch in: %r', host)
    es = Elasticsearch(hosts=[host])
    es.indices.delete(index=index_name, ignore=404)
    es.indices.create(index=index_name)

    logging.debug('Indexing %d documents...', len(documents))
    actions = [
        {
            '_op_type': 'index',
            '_index': index_name,
            '_type': document_type,
            '_source': document,
        }
        for document in documents
    ]
    with Timer() as index_timer:
        bulk(es, actions, refresh=True)
    logging.debug('Indexing took %f seconds', index_timer.elapsed)

    logging.debug('Running %d random search queries...', len(queries))
    with Timer() as query_timer:
        for query in queries:
            body = {
                'query': {
                    'match': {
                        'message': query
                    },
                },
                'size': 1,
                'highlight': {
                    'fields': {
                        'message': {},
                    },
                },
            }
            result = es.search(
                index=index_name,
                doc_type=document_type,
                body=body,
            )
            total = result['hits']['total']
            logging.debug('%r -> %d hits', query, total)
            if total > 0:
                logging.debug(pformat(result['hits']['hits'][0]['highlight']))
    logging.debug('Querying took %f seconds', query_timer.elapsed)
    return index_timer, query_timer


def postgresql(host, rows, queries):
    """Insert rows and run search queries in postgresql.

    :param host: PostgreSQL server location
    :type host: str
    :param rows: Rows to insert in the logs table
    :type count: list(dict(str))
    :type queries: Queries to execute
    :param queries: list(str)
    :returns: Insert and query timers
    :rtype: tuple(contexttimer.Timer, contexttimer.Timer)

    """
    table_name = 'logs'

    url = 'postgres://postgres@{}/database'.format(host)
    logging.debug('Connecting to postgresql in: %r...', url)
    engine = create_engine(url)
    if not database_exists(url):
        create_database(url)

    metadata = MetaData()
    metadata.bind = engine
    table = Table(
        table_name, metadata,
        Column('timestamp', TIMESTAMP, nullable=False),
        Column('message', TEXT, nullable=False),
        Column('vector', TSVECTOR, nullable=False),
    )
    Index(
        'message_index',
        func.to_tsvector('english', table.c.message),
        postgresql_using='gin',
    )
    metadata.drop_all()
    metadata.create_all()

    connection = engine.connect()

    logging.debug('Inserting %d rows...', len(rows))
    insert_query = text(
        'INSERT INTO logs (timestamp, message, vector) '
        'VALUES (:timestamp, :message, to_tsvector(:message))'
    )
    with Timer() as insert_timer:
        connection.execute(insert_query, rows)
    logging.debug('Inserting took %f seconds', insert_timer.elapsed)

    logging.debug('Running random search queries...')

    with Timer() as query_timer:
        for words in queries:
            plain_query = ' | '.join(words.split())
            select_query = text(
                "SELECT ts_headline('english', message, query) "
                "FROM ("
                    "SELECT message, query "
                    "FROM logs, to_tsquery('english', :plain_query) query "
                    "WHERE vector @@ query "
                    "ORDER BY ts_rank(vector, query) DESC "
                    "LIMIT 1"
                ") AS subquery"
            )
            result = connection.execute(
                select_query,
                {'plain_query': plain_query},
            )
            logging.debug('%r', plain_query)
            logging.debug(pformat(result.first()))
    logging.debug('Querying took %f seconds', query_timer.elapsed)
    return insert_timer, query_timer


def configure_logging(log_level):
    """Configure logging based on command line argument.

    :param log_level: Log level passed form the command line
    :type log_level: int

    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Log to sys.stderr using log level
    # passed through command line
    log_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    log_handler.setFormatter(formatter)
    log_handler.setLevel(log_level)
    root_logger.addHandler(log_handler)

    # Disable elasticsearch extra verbose logging
    logging.getLogger('elasticsearch').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.INFO)


def parse_arguments(argv):
    """Parse command line arguments.

    :returns: Parsed arguments
    :rtype: argparse.Namespace

    """
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        '-e', '--elasticsearch',
        default='127.0.0.1',
        help='Elasticsearch server (%(default)r by default)',
    )

    parser.add_argument(
        '-p', '--postgresql',
        default='127.0.0.1',
        help='PostgreSQL connection string (%(default)r by default)',
    )

    parser.add_argument(
        '--record-count',
        default=10000,
        type=int,
        help='Number of records to insert (%(default)s by default)',
    )

    parser.add_argument(
        '--query-count',
        default=10,
        type=int,
        help='Number of queries to execute (%(default)s by default)',
    )

    log_levels = ['debug', 'info', 'warning', 'error', 'critical']
    parser.add_argument(
        '-l', '--log-level',
        dest='log_level',
        choices=log_levels,
        default='debug',
        help=('Log level. One of {0} or {1} '
              '(%(default)s by default)'
              .format(', '.join(log_levels[:-1]), log_levels[-1])))

    args = parser.parse_args(argv)
    args.log_level = getattr(logging, args.log_level.upper())
    return args


if __name__ == '__main__':
    main()
