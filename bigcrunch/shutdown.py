import asyncio

from bigcrunch import webapp


@asyncio.coroutine
def shutdown():
    client = yield from webapp.redshift_client()
    cluster_control = webapp.ClusterControl(client)
    cluster = yield from cluster_control.get()

    engine = yield from webapp.create_engine(cluster)
    with (yield from engine) as conn:
        db = webapp.Database(conn=conn)
        sessions = yield from db.running_test_sessions()

    if sessions == 0:
        yield from cluster_control.destroy()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(shutdown())
    loop.close()
