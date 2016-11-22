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
from matplotlib import pyplot as plt
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

    results = {}
    for record_count in args.record_count:
        results[record_count] = run_single_test(args, record_count)

    es_query = [
        results[record_count]['es']['query'].elapsed / args.query_count
        for record_count in args.record_count
    ]
    pg_query = [
        results[record_count]['pg']['query'].elapsed / args.query_count
        for record_count in args.record_count
    ]
    plot_results(args.record_count, es_query, pg_query)


def run_single_test(args, record_count):
    """Run a single test.

    :param args: Command line arguments
    :type args: argparse.Namespace
    :param record_count: Number of records to insert in this run
    :type record_count
    :returns: Elasticsearch and PosgreSQL timers
    :rtype: dict(str, dict(str, contexttimer.Timer))

    """
    records, queries = generate_random_data(
        record_count, args.query_count)
    es_timer = elasticsearch(args.elasticsearch, records, queries)
    pg_timer = postgresql(args.postgresql, records, queries, args.hits)
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
        es_timer['index'].elapsed,
        es_timer['query'].elapsed / args.query_count,
        es_timer['query'].elapsed,
        pg_timer['insert'].elapsed,
        pg_timer['query'].elapsed / args.query_count,
        pg_timer['query'].elapsed,
    )

    return {'es': es_timer, 'pg': pg_timer}


def plot_results(record_count, es_query, pg_query):
    """Plot full-text search performance.

    :param record_count:
        Each of the record counts for which a test was executed
    :type record_count: list(int)
    :param es_query: ElasticSearch query times
    :type es_query: list(int)
    :param pg_query: PostgreSQL query times
    :type pg_query: list(int)

    """
    plt.plot(record_count, es_query, label='Elasticsearch')
    plt.plot(record_count, pg_query, label='PostgreSQL')
    plt.title('Full-text search performance')
    plt.xlabel('Record count')
    plt.ylabel('Time (s)')
    plt.xscale('log')
    plt.legend(loc='upper left')
    plt.show()


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
    :rtype: dict(str, contexttimer.Timer)

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
    return {'index': index_timer, 'query': query_timer}


def postgresql(host, rows, queries, include_hits):
    """Insert rows and run search queries in postgresql.

    :param host: PostgreSQL server location
    :type host: str
    :param rows: Rows to insert in the logs table
    :type count: list(dict(str))
    :param queries: Queries to execute
    :type queries: list(str)
    :param include_hits: Include hits in results
    :type include_hits: bool
    :returns: Insert and query timers
    :rtype: dict(str, contexttimer.Timer)

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

    if include_hits:
        select_query = text(
            "WITH matches AS ("
                "SELECT message, query "
                "FROM logs, to_tsquery('english', :plain_query) AS query "
                "WHERE vector @@ query "
                "ORDER BY ts_rank(vector, query) DESC "
            ")"
            "SELECT "
                "(SELECT COUNT(*) FROM matches) AS count, "
                "ts_headline('english', message, query) "
            "FROM matches "
            "LIMIT 1"
        )
    else:
        select_query = text(
            "SELECT ts_headline('english', message, query) "
            "FROM ("
                "SELECT message, query "
                "FROM logs, to_tsquery('english', :plain_query) AS query "
                "WHERE vector @@ query "
                "ORDER BY ts_rank(vector, query) DESC "
                "LIMIT 1"
            ") AS subquery"
        )

    with Timer() as query_timer:
        for words in queries:
            plain_query = ' | '.join(words.split())
            result = connection.execute(
                select_query,
                {'plain_query': plain_query},
            ).fetchone()

            if include_hits:
                if result:
                    total, highlight = result
                else:
                    total, highlight = 0, None
                logging.debug('%r -> %d hits', plain_query, total)
            else:
                if result:
                    highlight = result[0]
                logging.debug('%r', plain_query)
            if highlight is not None:
                logging.debug(pformat(highlight))
    logging.debug('Querying took %f seconds', query_timer.elapsed)
    return {'insert': insert_timer, 'query': query_timer}


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
        '--hits',
        action='store_true',
        help='Include hits in results (only affects PostgreSQL)',
    )

    default_record_count = [10000]
    parser.add_argument(
        '--record-count',
        default='10000',
        help=(
            'Comma separated list of number of records '
            'to insert in each run ({!r} by default)'
            .format(default_record_count)
        ),
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
    args.record_count = map(int, args.record_count.split(','))
    args.log_level = getattr(logging, args.log_level.upper())
    return args


if __name__ == '__main__':
    main()
