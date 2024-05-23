import tornado.ioloop
import tornado.web
import uuid
from cassandra.cluster import Cluster, Session  # type: ignore
from datetime import datetime


class BaseHandler(tornado.web.RequestHandler):
    def initialize(self, cassandra_session: Session):
        self.cassandra_session = cassandra_session


class MakeReservationHandler(BaseHandler):
    async def post(self):
        book_id = int(self.get_argument("book_id"))
        customer_id = int(self.get_argument("customer_id"))
        reservation_id = uuid.uuid4()
        reservation_date = int(datetime.now().timestamp() * 1000)

        query = (
            "INSERT INTO reservations (book_id, customer_id, reservation_date, reservation_id) "
            "VALUES (%s, %s, %s, %s)"
            "IF NOT EXISTS;"
        )
        result = self.cassandra_session.execute(
            query, (book_id, customer_id, reservation_date, reservation_id)
        ).one()
        if result.applied:
            self.write({"status": "success", "reservation_id": str(reservation_id)})
        else:
            self.write({"status": "already_reserved"})


class UpdateReservationHandler(BaseHandler):
    async def post(self):
        book_id = int(self.get_argument("book_id"))
        customer_id = int(self.get_argument("customer_id"))
        reservation_date = datetime.now().timestamp() * 1000

        query = "UPDATE reservations SET reservation_date = %s WHERE book_id = %s and customer_id = %s;"
        self.cassandra_session.execute(
            query, (int(reservation_date), book_id, customer_id)
        )
        self.write({"status": "success", "reservation_date": reservation_date})


class ViewReservationHandler(BaseHandler):
    async def get(self):
        book_id = int(self.get_argument("book_id"))

        query = "SELECT * FROM reservations WHERE book_id = %s;"
        reservation = self.cassandra_session.execute(query, (book_id,)).one()

        if reservation:
            self.write(
                {
                    "reservation_id": str(reservation.reservation_id),
                    "book_id": reservation.book_id,
                    "customer_id": reservation.customer_id,
                    "reservation_date": str(reservation.reservation_date),
                }
            )
        else:
            self.write({"status": "not found"})


class ListReservationHandler(BaseHandler):
    async def get(self):
        query = "SELECT * FROM reservations;"
        reservations = self.cassandra_session.execute(query)
        result = []
        for reservation in reservations:
            result.append(
                {
                    "reservation_id": str(reservation.reservation_id),
                    "book_id": reservation.book_id,
                    "customer_id": reservation.customer_id,
                    "reservation_date": str(reservation.reservation_date),
                }
            )
        self.write({"reservations": result})


def make_app(cassandra_session, debug=False):
    return tornado.web.Application(
        [
            (
                r"/make_reservation",
                MakeReservationHandler,
                dict(cassandra_session=cassandra_session),
            ),
            (
                r"/update_reservation",
                UpdateReservationHandler,
                dict(cassandra_session=cassandra_session),
            ),
            (
                r"/view_reservation",
                ViewReservationHandler,
                dict(cassandra_session=cassandra_session),
            ),
            (
                r"/list_reservations",
                ListReservationHandler,
                dict(cassandra_session=cassandra_session),
            ),
        ],
        debug=debug,
    )


if __name__ == "__main__":
    cluster = Cluster(
        ["127.0.0.1"], port=9042
    )  # Replace with your Cassandra cluster nodes
    session = cluster.connect("library")

    app = make_app(session, debug=False)
    app.listen(8888)
    print("Started listening...")
    tornado.ioloop.IOLoop.current().start()
