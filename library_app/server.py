import tornado.ioloop
import tornado.web
import uuid
from cassandra.query import PreparedStatement  # type: ignore
from cassandra.cluster import Cluster, Session  # type: ignore
from datetime import datetime
import time
from dataclasses import dataclass


@dataclass
class PreparedStatements:
    select_reservation: PreparedStatement
    insert_reservation: PreparedStatement


class BaseHandler(tornado.web.RequestHandler):
    def initialize(
        self, cassandra_session: Session, prepared_statements: PreparedStatements
    ):
        self.cassandra_session = cassandra_session
        self.prepared_statements = prepared_statements


class MakeReservationHandler(BaseHandler):
    async def post(self):
        book_id = int(self.get_argument("book_id"))
        customer_id = int(self.get_argument("customer_id"))
        reservation_id = uuid.uuid4()
        reservation_date = int(datetime.now().timestamp() * 1000)

        future = self.cassandra_session.execute_async(
            self.prepared_statements.insert_reservation,
            (book_id, customer_id, reservation_date, reservation_id),
        )

        result = future.result().one()
        if result.applied:
            self.write({"status": "success", "reservation_id": str(reservation_id)})
        else:
            self.write({"status": "already_reserved"})


class UpdateReservationHandler(BaseHandler):
    async def post(self):
        # book_id = int(self.get_argument("book_id"))
        # customer_id = int(self.get_argument("customer_id"))
        # reservation_date = datetime.now().timestamp() * 1000

        # query = "UPDATE reservations SET reservation_date = %s WHERE book_id = %s and customer_id = %s;"
        # self.cassandra_session.execute(
        #     query, (int(reservation_date), book_id, customer_id)
        # )
        # self.write({"status": "success", "reservation_date": reservation_date})
        time.sleep(10)
        self.write({"status": "success"})


class ViewReservationHandler(BaseHandler):
    async def get(self):
        book_id = int(self.get_argument("book_id"))

        future = self.cassandra_session.execute_async(
            self.prepared_statements.select_reservation, (book_id,)
        )
        result = future.result().one()

        if result:
            self.write(
                {
                    "reservation_id": str(result.reservation_id),
                    "book_id": result.book_id,
                    "customer_id": result.customer_id,
                    "reservation_date": str(result.reservation_date),
                }
            )
        else:
            self.write({"status": "not found"})


class ListReservationHandler(BaseHandler):
    async def get(self):
        query = "SELECT * FROM reservations;"

        future = self.cassandra_session.execute_async(query)
        result = future.result().all()
        reservations = []
        for reservation in result:
            reservations.append(
                {
                    "reservation_id": str(reservation.reservation_id),
                    "book_id": reservation.book_id,
                    "customer_id": reservation.customer_id,
                    "reservation_date": str(reservation.reservation_date),
                }
            )
        self.write({"reservations": reservations})


def make_app(cassandra_session, prepared_statements, debug=False):
    return tornado.web.Application(
        [
            (
                r"/make_reservation",
                MakeReservationHandler,
                dict(
                    cassandra_session=cassandra_session,
                    prepared_statements=prepared_statements,
                ),
            ),
            (
                r"/update_reservation",
                UpdateReservationHandler,
                dict(
                    cassandra_session=cassandra_session,
                    prepared_statements=prepared_statements,
                ),
            ),
            (
                r"/view_reservation",
                ViewReservationHandler,
                dict(
                    cassandra_session=cassandra_session,
                    prepared_statements=prepared_statements,
                ),
            ),
            (
                r"/list_reservations",
                ListReservationHandler,
                dict(
                    cassandra_session=cassandra_session,
                    prepared_statements=prepared_statements,
                ),
            ),
        ],
        debug=debug,
    )


if __name__ == "__main__":
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect("library")

    prepared_statements = PreparedStatements(
        select_reservation=session.prepare(
            "SELECT * FROM reservations WHERE book_id = ?;"
        ),
        insert_reservation=session.prepare(
            "INSERT INTO reservations (book_id, customer_id, reservation_date, reservation_id) "
            "VALUES (?, ?, ?, ?)"
            "IF NOT EXISTS;"
        ),
    )

    app = make_app(session, prepared_statements, debug=False)
    app.listen(8888)
    print("Started listening...")
    tornado.ioloop.IOLoop.current().start()
