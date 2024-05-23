# type: ignore
import os

from cassandra.cluster import (
    Cluster,
    ExecutionProfile,
    EXEC_PROFILE_DEFAULT,
    ConsistencyLevel,
)


KEYSPACE_NAME = os.environ.get("KEYSPACE_NAME", "library")


def main():
    exec_profiles = {
        EXEC_PROFILE_DEFAULT: ExecutionProfile(
            consistency_level=ConsistencyLevel.QUORUM, request_timeout=50
        )
    }

    cluster = Cluster(["127.0.0.1"], port=9042, execution_profiles=exec_profiles)
    session = cluster.connect()

    queries = [
        f"DROP KEYSPACE IF EXISTS {KEYSPACE_NAME};",
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};",
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.reservations "
        "(reservation_id uuid, book_id int, customer_id int, reservation_date timestamp, PRIMARY KEY(book_id));",
        f"TRUNCATE TABLE {KEYSPACE_NAME}.reservations;",
    ]

    for query in queries:
        session.execute(query)


if __name__ == "__main__":
    main()
