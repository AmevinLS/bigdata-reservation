# type: ignore
import os

from cassandra.cluster import (
    Cluster,
    Session,
    ExecutionProfile,
    BatchStatement,
    EXEC_PROFILE_DEFAULT,
    ConsistencyLevel,
)
import time
import logging


KEYSPACE_NAME = os.environ.get("KEYSPACE_NAME", "library")
BOOKS = [
    ("Things Fall Apart", "Chinua Achebe"),
    ("Fairy tales", "Hans Christian Andersen"),
    ("The Divine Comedy", "Dante Alighieri"),
    ("The Epic Of Gilgamesh", "Unknown"),
    ("The Book Of Job", "Unknown"),
    ("One Thousand and One Nights", "Unknown"),
    ("Njál's Saga", "Unknown"),
    ("Pride and Prejudice", "Jane Austen"),
    ("Le Père Goriot", " Honoré de Balzac"),
    ("Molloy, Malone Dies, The Unnamable, the trilogy", "Samuel Beckett"),
    ("The Decameron", "Giovanni Boccaccio"),
    ("Ficciones", "Jorge Luis Borges"),
    ("Wuthering Heights", "Emily Brontë"),
    ("The Stranger", "Albert Camus"),
    ("Poems", "Paul Celan"),
    ("Journey to the End of the Night", "Louis-Ferdinand Céline"),
    ("Don Quijote De La Mancha", "Miguel de Cervantes"),
    ("The Canterbury Tales", "Geoffrey Chaucer"),
    ("Stories", "Anton Chekhov"),
    ("Nostromo", "Joseph Conrad"),
]


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
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE_NAME}.books "
        "(book_id int, title text, author text, reservation_id uuid, PRIMARY KEY(book_id));",
    ]

    for query in queries:
        logging.info(f"Executing query: {query}")
        session.execute(query)

    logging.info("Ensuring schema equality")
    ensure_schemas_equality(nodes, verbose=False)

    batch = BatchStatement()
    for i, (title, author) in enumerate(BOOKS):
        batch.add(
            f"INSERT INTO {KEYSPACE_NAME}.books (book_id, title, author) VALUES (%s, %s, %s);",
            (i, title, author),
        )
    logging.info("Batch inserting books")
    session.execute(batch)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
