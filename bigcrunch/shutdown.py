import asyncio

from yieldfrom.botocore import exceptions as botocore_exceptions

from bigcrunch import webapp


@asyncio.coroutine
def shutdown():
    client = yield from webapp.redshift_client()
    cluster_control = webapp.ClusterControl(client)
    try:
        cluster = yield from cluster_control.get()
    except botocore_exceptions.ClientError as e:
        if e.response['Error']['Code'] != 'ClusterNotFound':
            raise e
        else:
            print('Redshift already shutdown')
            return

    engine = yield from webapp.create_engine(cluster)
    with (yield from engine) as conn:
        db = webapp.Database(conn=conn)
        sessions = yield from db.running_test_sessions()

    if sessions == 0:
        print('shutting down Redshift')
        yield from cluster_control.destroy()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(shutdown())
    loop.close()
