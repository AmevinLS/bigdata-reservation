import os

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel # type: ignore


KEYSPACE_NAME = os.environ.get("KEYSPACE_NAME", "library")


def main():
    exec_profiles = {
        EXEC_PROFILE_DEFAULT: ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM, request_timeout=50)
    }

    cluster = Cluster(["127.0.0.1"], port=9042, execution_profiles=exec_profiles)
    session = cluster.connect()

    session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE_NAME};")
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE_NAME} "
        "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    )

    session.execute(
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.reservations "
        "(reservation_id uuid, book_id int, customer_id int, reservation_date timestamp, PRIMARY KEY(book_id, customer_id));"
    )
    session.execute(f"TRUNCATE TABLE {KEYSPACE_NAME}.reservations;")


if __name__ == "__main__":
    main()
