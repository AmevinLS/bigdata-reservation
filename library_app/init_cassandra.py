# type: ignore
import os

from cassandra.cluster import (
    Cluster,
    Session,
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
    ConsistencyLevel,
)
import time
import logging


KEYSPACE_NAME = os.environ.get("KEYSPACE_NAME", "library")


def get_schema_version(session: Session):
    query = "SELECT schema_version FROM system.local;"
    result = session.execute(query).one()
    return result.schema_version


def ensure_schemas_equality(nodes: list[str], verbose: bool = False):
    clusters = [Cluster([node]) for node in nodes]
    sessions = [cluster.connect() for cluster in clusters]

    while True:
        schema_versions = []
        for session in sessions:
            schema_version = get_schema_version(session)
            schema_versions.append(schema_version)

        if verbose:
            print(f"{schema_versions = }")
        if len(set(schema_versions)) == 1:
            break
        time.sleep(1)

    for session, cluster in zip(sessions, clusters):
        session.shutdown()
        cluster.shutdown()


def main():
    exec_profiles = {
        EXEC_PROFILE_DEFAULT: ExecutionProfile(
            consistency_level=ConsistencyLevel.ALL, request_timeout=200
        )
    }

    nodes = ["127.0.0.1", "127.0.0.2"]
    cluster = Cluster(nodes, port=9042, execution_profiles=exec_profiles)
    session = cluster.connect()

    queries = [
        f"DROP KEYSPACE IF EXISTS {KEYSPACE_NAME};",
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2};",
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.reservations_by_book_id "
        "(book_id int, customer_id int, reservation_id uuid, reservation_date timestamp, PRIMARY KEY(book_id));",
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.reservations_by_id "
        "(book_id int, customer_id int, reservation_id uuid, reservation_date timestamp, PRIMARY KEY(reservation_id));",
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.reservations_by_customer_id "
        "(book_id int, customer_id int, reservation_id uuid, reservation_date timestamp, PRIMARY KEY(customer_id, book_id));",
    ]

    for query in queries:
        session.execute(query)
        logging.info(f"Executed query: {query}")

    ensure_schemas_equality(nodes, verbose=False)
    logging.info("Schema equality ensured")


if __name__ == "__main__":
    main()
