from uuid import UUID
from cassandra.cluster import Cluster  # type: ignore


class LibraryWrapper:
    def __init__(self):
        self.cluster = Cluster(["127.0.0.1", "127.0.0.2"], port=9042)
        self.db = self.cluster.connect("library")

        self.select_count_reservations = self.db.prepare(
            "SELECT COUNT(*) FROM reservations_by_customer_id WHERE customer_id = ?;"
        )
        self.select_reservation_by_book_id = self.db.prepare(
            "SELECT * FROM reservations_by_book_id WHERE book_id = ?;"
        )
        self.insert_reservation_by_book_id = self.db.prepare(
            "INSERT INTO reservations_by_book_id (book_id, customer_id, reservation_id, reservation_date) "
            "VALUES (?, ?, ?, ?) IF NOT EXISTS;",
        )
        self.insert_reservation_by_id = self.db.prepare(
            "INSERT INTO reservations_by_id (book_id, customer_id, reservation_id, reservation_date) VALUES (?, ?, ?, ?);"
        )
        self.insert_reservation_by_customer_id = self.db.prepare(
            "INSERT INTO reservations_by_customer_id (book_id, customer_id, reservation_id, reservation_date) VALUES (?, ?, ?, ?);"
        )
        self.select_reservation_by_customer_id = self.db.prepare(
            "SELECT * FROM reservations_by_customer_id WHERE customer_id = ?;"
        )
        self.select_all_reservations = self.db.prepare(
            "SELECT * FROM reservations_by_customer_id;"
        )

    async def get_books(self, only_available: bool = False):
        books_result = self.db.execute_async("SELECT * FROM books;").result().all()
        if only_available:
            books = [
                {"book_id": book.book_id, "title": book.title, "author": book.author}
                for book in books_result
                if book.reservation_id is None
            ]
        else:
            books = [
                {
                    "book_id": book.book_id,
                    "title": book.title,
                    "author": book.author,
                    "reservation_id": book.reservation_id,
                }
                for book in books_result
            ]
        return books

    async def get_reservation_count(self, customer_id: int) -> int:
        count = (
            self.db.execute_async(
                self.select_count_reservations,
                (customer_id,),
            )
            .result()
            .one()
            .count
        )
        return count

    async def insert_reservation(
        self,
        book_id: int,
        customer_id: int,
        reservation_id: UUID,
        reservation_date: int,
    ) -> bool:
        applied = (
            self.db.execute_async(
                self.insert_reservation_by_book_id,
                (book_id, customer_id, reservation_id, reservation_date),
            )
            .result()
            .one()
        ).applied

        if applied:
            self.db.execute_async(
                self.insert_reservation_by_id,
                (book_id, customer_id, reservation_id, reservation_date),
            )
            self.db.execute_async(
                self.insert_reservation_by_customer_id,
                (book_id, customer_id, reservation_id, reservation_date),
            )
            self.db.execute_async(
                "UPDATE books SET reservation_id = %s WHERE book_id = %s;",
                (reservation_id, book_id),
            )

        return applied

    async def update_reservation(
        self, reservation_date: int, book_id: int, customer_id: int
    ) -> bool:
        applied = (
            self.db.execute_async(
                "UPDATE reservations_by_customer_id SET reservation_date = %s "
                "WHERE book_id = %s AND customer_id = %s IF reservation_date < %s;",
                (reservation_date, book_id, customer_id, reservation_date),
            )
            .result()
            .one()
            .applied
        )

        if applied:
            reserv_result = (
                self.db.execute_async(
                    "SELECT reservation_id FROM reservations_by_customer_id "
                    "WHERE customer_id = %s AND book_id = %s;",
                    (customer_id, book_id),
                )
                .result()
                .one()
            )

            self.db.execute_async(
                "UPDATE reservations_by_book_id SET reservation_date = %s "
                "WHERE book_id = %s;",
                (reservation_date, book_id),
            )
            self.db.execute_async(
                "UPDATE reservations_by_id SET reservation_date = %s "
                "WHERE reservation_id = %s;",
                (reservation_date, reserv_result.reservation_id),
            )

        return applied

    async def get_reservation(self, book_id: int):
        result = (
            self.db.execute_async(self.select_reservation_by_book_id, (book_id,))
            .result()
            .one()
        )
        if result is not None:
            return {
                "reservation_id": str(result.reservation_id),
                "book_id": result.book_id,
                "customer_id": result.customer_id,
                "reservation_date": str(result.reservation_date),
            }
        else:
            return None

    async def get_reservations(self, customer_id: int | None):
        if customer_id is not None:
            result = (
                self.db.execute_async(
                    self.select_reservation_by_customer_id,
                    (customer_id,),
                )
                .result()
                .all()
            )
        else:
            result = self.db.execute_async(self.select_all_reservations).result().all()
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
        return reservations

    async def clear_reservations(self):
        timeout = 30
        self.db.execute("TRUNCATE TABLE reservations_by_book_id;", timeout=timeout)
        self.db.execute("TRUNCATE TABLE reservations_by_id;", timeout=timeout)
        self.db.execute("TRUNCATE TABLE reservations_by_customer_id;", timeout=timeout)
        book_ids = tuple([str(book["book_id"]) for book in await self.get_books()])
        self.db.execute(
            f"UPDATE books SET reservation_id = null WHERE book_id IN ({','.join(book_ids)});",
            timeout=timeout,
        )
