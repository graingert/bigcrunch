import asyncio
import json
import os
import uuid
from http import client as http_client

import sqlalchemy as sa
from aiohttp import web
from aiopg import sa as aiopg_sa
from yieldfrom.botocore import exceptions as botocore_exceptions
from yieldfrom.botocore import session as botocore_session


create_table = """
CREATE TABLE IF NOT EXISTS test_session (
    pk VARCHAR DISTKEY SORTKEY NOT NULL,
    created_time TIMESTAMP DEFAULT SYSDATE NOT NULL,
    PRIMARY KEY (pk)
)
"""


class ClusterControl(object):
    cluster_name = 'redshift-sqlalchemy-test'
    username = 'travis'
    password = os.environ['PGPASSWORD']

    def __init__(self, client):
        self.client = client

    @asyncio.coroutine
    def get_or_create(self):
        cluster_identifier = self.cluster_name
        client = self.client
        username = self.username
        password = self.password

        try:
            response = yield from client.create_cluster(
                ClusterIdentifier=cluster_identifier,
                NodeType='dc1.large',
                MasterUsername=username,
                MasterUserPassword=password,
            )
        except botocore_exceptions.ClientError as e:
            if e.response['Error']['Code'] != 'ClusterAlreadyExists':
                raise e
            return (yield from self.get())
        else:
            return response['Cluster']['Endpoint']

    @asyncio.coroutine
    def get(self):
        client = self.client
        cluster_identifier = self.cluster_name
        response = yield from client.describe_clusters(
            ClusterIdentifier=cluster_identifier,
        )
        return response['Clusters'][0]['Endpoint']

    @asyncio.coroutine
    def destroy(self):
        raise Exception()
        cluster_identifier = self.cluster_name
        client = self.client
        try:
            yield from client.delete_cluster(
                ClusterIdentifier=cluster_identifier,
                SkipFinalClusterSnapshot=True,
            )
        except botocore_exceptions.ClientError as e:
            if e.response['Error']['Code'] != 'InvalidClusterState':
                raise e


class Database(object):
    def __init__(self, conn):
        self.conn = conn

    @asyncio.coroutine
    def add_test_session(self, uuid):
        trans = yield from self.conn.begin()
        try:
            yield from self.conn.execute(create_table)
            insert = sa.text("""
                INSERT INTO test_session (pk)
                VALUES (:pk)
            """)
            yield from self.conn.execute(insert, pk=uuid)
        except:
            yield from trans.rollback()
            raise
        else:
            yield from trans.commit()

    def remove_test_session(self, uuid):
        trans = yield from self.conn.begin()
        try:
            yield from self.conn.execute(create_table)
            delete = sa.text("""
                DELETE FROM test_session
                WHERE pk = :pk
            """)
            yield from self.conn.execute(delete, pk=uuid)
        except:
            yield from trans.rollback()
            raise
        else:
            yield from trans.commit()

    def running_test_sessions(self):
        query = """
        SELECT COUNT(*) AS sessions
        FROM test_session
        WHERE (created_time + interval '1 hour') < SYSDATE
        """
        result = yield from self.conn.execute(query)
        return (yield from result.fetchone()).sessions


def create_engine(cluster):
    return aiopg_sa.create_engine(
        user=ClusterControl.username,
        password=ClusterControl.password,
        host=cluster['Address'],
        port=cluster['Port'],
        dbname='dev',
        client_encoding='utf8',
        enable_hstore=False,
    )


def redshift_client():
    session = botocore_session.get_session()
    return session.create_client('redshift')


@asyncio.coroutine
def create_database(request):
    session_id = str(uuid.uuid1())

    client = yield from redshift_client()
    cluster = yield from ClusterControl(client).get_or_create()

    engine = yield from create_engine(cluster)
    with (yield from engine) as conn:
        db = Database(conn=conn)
        yield from db.add_test_session(session_id)

    return web.Response(
        text=json.dumps({
            'resource_url': '/session/' + session_id,
            'cluster': cluster,
        }),
    )


@asyncio.coroutine
def delete_database(request):
    session_id = request.match_info['session_id']

    client = yield from redshift_client()
    cluster = yield from ClusterControl(client).get()

    engine = yield from create_engine(cluster)
    with (yield from engine) as conn:
        db = Database(conn=conn)
        yield from db.remove_test_session(session_id)

    return web.Response(
        text='',
        status=http_client.NO_CONTENT,
    )


def index(request):
    return web.Response(
        text=json.dumps({'testSessions': '/session/'}),
    )


@asyncio.coroutine
def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    app.router.add_route('POST', '/session/', create_database)
    app.router.add_route('DELETE', '/session/{session_id}', delete_database)

    srv = yield from loop.create_server(
        protocol_factory=app.make_handler(),
        host=None,
        port=os.environ['PORT'],
    )
    return srv

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init(loop))
    loop.run_forever()
    loop.close()
