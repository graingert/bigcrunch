import asyncio
import json
import os
import uuid
from http import client as http_client

import sqlalchemy as sa
import aiobotocore
from aiohttp import web
from aiopg import sa as aiopg_sa
import botocore.exceptions
import psycopg2

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
    vpc_security_group_id = os.environ['VPC_SECURITY_GROUP_ID']
    iam_role = os.environ['IAM_ROLE_ARN']

    def __init__(self, client):
        self.client = client

    @asyncio.coroutine
    def get_or_create(self):
        cluster_identifier = self.cluster_name
        client = self.client
        username = self.username
        password = self.password
        vpc_security_group_id = self.vpc_security_group_id
        iam_role = self.iam_role

        while True:
            try:
                response = yield from client.create_cluster(
                    ClusterIdentifier=cluster_identifier,
                    NodeType='dc2.large',
                    ClusterType='single-node',
                    VpcSecurityGroupIds=[vpc_security_group_id],
                    MasterUsername=username,
                    MasterUserPassword=password,
                    PubliclyAccessible=True,
                    ClusterParameterGroupName='default.redshift-1.0',
                    IamRoles=[iam_role],
                )
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] != 'ClusterAlreadyExists':
                    raise e
                return (yield from self.get())
            else:
                cluster = response['Cluster']
                status = cluster['ClusterStatus']
                if status == 'deleting':
                    print('cluster is deleting')
                    yield from asyncio.sleep(15)
                    continue
                if status != 'available':
                    print('cluster is {status}'.format(status=status))
                    print(response)
                    yield from asyncio.sleep(5)
                    return (yield from self.get())
                if 'Address' not in cluster['Endpoint']:
                    print('Something went wrong')
                    print(response)
                    yield from asyncio.sleep(5)
                    return (yield from self.get())
                return cluster['Endpoint']

    @asyncio.coroutine
    def get(self):
        client = self.client
        cluster_identifier = self.cluster_name
        not_found_count = 0
        while True:
            try:
                response = yield from client.describe_clusters(
                    ClusterIdentifier=cluster_identifier,
                )
            except botocore.exceptions.ClientError as e:
                not_found_and_retry = (
                    e.response['Error']['Code'] == 'ClusterNotFound' and
                    not_found_count < 0
                )
                if not_found_and_retry:
                    not_found_count += 1
                    print('cluster not found yet')
                    print(e.response)
                    yield from asyncio.sleep(15)
                else:
                    raise e
            else:
                cluster = response['Clusters'][0]
                if cluster['ClusterStatus'] == 'creating':
                    print('Cluster is still creating')
                    print(response)
                    yield from asyncio.sleep(5)
                    continue
                if 'Address' not in cluster['Endpoint']:
                    print('Something went wrong')
                    print(response)
                    yield from asyncio.sleep(5)
                    continue
                else:
                    return cluster['Endpoint']

    @asyncio.coroutine
    def destroy(self):
        cluster_identifier = self.cluster_name
        client = self.client
        try:
            yield from client.delete_cluster(
                ClusterIdentifier=cluster_identifier,
                SkipFinalClusterSnapshot=True,
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] != 'InvalidClusterState':
                raise e


class Database(object):
    def __init__(self, conn):
        self.conn = conn

    @asyncio.coroutine
    def add_test_session(self, uuid):
        while True:
            trans = yield from self.conn.begin()
            try:
                yield from self.conn.execute(create_table)
                insert = sa.text("""
                    INSERT INTO test_session (pk)
                    VALUES (:pk)
                """)
                yield from self.conn.execute(insert, pk=uuid)
            except psycopg2.IntegrityError:
                yield from trans.rollback()
            except:
                yield from trans.rollback()
                raise
            else:
                yield from trans.commit()
                return

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
        WHERE (created_time + interval '4 hour') < SYSDATE
        """
        try:
            result = yield from self.conn.execute(query)
            return (yield from result.fetchone()).sessions
        # Table doesn't exist
        except psycopg2.ProgrammingError:
            return 0


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
    session = aiobotocore.get_session()
    return session.create_client('redshift')


@asyncio.coroutine
def create_database(request):
    session_id = str(uuid.uuid1())

    client = redshift_client()
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

    client = redshift_client()
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
