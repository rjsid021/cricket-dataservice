from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from common.db_config import HOST, PORT, DB_NAME, DB_USER, DB_PASSWORD
from log.log import get_logger
from cassandra.auth import PlainTextAuthProvider


def createCassandraSession(host_name, port_name, db, username, password):
    lbp = RoundRobinPolicy()
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster([host_name], port_name, protocol_version=4, idle_heartbeat_interval=600,
                      load_balancing_policy=lbp, auth_provider=auth_provider)
    connect = cluster.connect(db, wait_for_all_pools=True)
    return connect


logger = get_logger("root", "session")
session = createCassandraSession(HOST, PORT, DB_NAME, DB_USER, DB_PASSWORD)
logger.debug("Session --> {}".format(session))

# session.execute("USE {}".format(DB_NAME))
# rows = session.execute('SELECT * FROM Teams')
# print(rows)
# for row in rows:
#     print(row)
