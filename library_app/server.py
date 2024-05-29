from fastapi import FastAPI, HTTPException, Response
import uvicorn

from uuid import uuid4
from datetime import datetime

from cassandra.cluster import Cluster  # type: ignore


RESERVATION_PER_USER_LIMIT = 500

cluster = Cluster(["127.0.0.1", "127.0.0.2"], port=9042)
db = cluster.connect("library")

select_count_reservations = db.prepare(
    "SELECT COUNT(*) FROM reservations_by_customer_id WHERE customer_id = ?;"
)
insert_reservation_by_book_id = db.prepare(
    "INSERT INTO reservations_by_book_id (book_id, customer_id, reservation_id, reservation_date) "
    "VALUES (?, ?, ?, ?) IF NOT EXISTS;",
)
insert_reservation_by_id = db.prepare(
    "INSERT INTO reservations_by_id (book_id, customer_id, reservation_id, reservation_date) VALUES (?, ?, ?, ?);"
)
insert_reservation_by_customer_id = db.prepare(
    "INSERT INTO reservations_by_customer_id (book_id, customer_id, reservation_id, reservation_date) VALUES (?, ?, ?, ?);"
)

app = FastAPI()


@app.post("/make_reservation")
async def make_reservation(book_id: int, customer_id: int):
    reservation_id = uuid4()
    reservation_date_obj = datetime.now()
    reservation_date = int(reservation_date_obj.timestamp() * 1000)

    count_result = (
        db.execute_async(
            select_count_reservations,
            (customer_id,),
        )
        .result()
        .one()
    )
    if count_result.count >= RESERVATION_PER_USER_LIMIT:
        raise HTTPException(401, "Reservation limit reached")

    # Insert LWT
    result = (
        db.execute_async(
            insert_reservation_by_book_id,
            (book_id, customer_id, reservation_id, reservation_date),
        )
        .result()
        .one()
    )
    if not result.applied:
        raise HTTPException(400, "Book already reserved.")

    # Check if reservation already exists for book
    # prev_reservs = (
    #     db.execute_async(
    #         "SELECT * FROM reservations_by_book_id WHERE book_id = %s;", (book_id,)
    #     )
    #     .result()
    #     .all()
    # )
    # if len(prev_reservs) > 0:
    #     raise HTTPException(400, "Book already reserved")

    # # Insert reservation
    # db.execute_async(
    #     "INSERT INTO reservations_by_book_id (book_id, customer_id, reservation_id, reservation_date) VALUES (%s, %s, %s, %s);",
    #     (book_id, customer_id, reservation_id, reservation_date),
    # ).result()

    # # Check if the reservation in the table is ours
    # new_reserv = None
    # while new_reserv is None:
    #     new_reserv = (
    #         db.execute_async(
    #             "SELECT * FROM reservations_by_book_id WHERE book_id = %s;", (book_id,)
    #         )
    #         .result()
    #         .one()
    #     )
    # if new_reserv.reservation_id != reservation_id:
    #     raise HTTPException(400, "Book already reserved")

    db.execute_async(
        insert_reservation_by_id,
        (book_id, customer_id, reservation_id, reservation_date),
    )
    db.execute_async(
        insert_reservation_by_customer_id,
        (book_id, customer_id, reservation_id, reservation_date),
    )

    return {"status": "success", "reservation_id": str(reservation_id)}


@app.post("/update_reservation")
async def update_reservation(book_id: int, customer_id: int):
    reservation_date_obj = datetime.now()
    reservation_date = int(reservation_date_obj.timestamp() * 1000)

    result = (
        db.execute_async(
            "UPDATE reservations_by_customer_id SET reservation_date = %s "
            "WHERE book_id = %s AND customer_id = %s IF reservation_date < %s;",
            (reservation_date, book_id, customer_id, reservation_date),
        )
        .result()
        .one()
    )
    if not result.applied:
        raise HTTPException(
            402, "Cannot update reservation with earlier 'reservation_date'"
        )

    reserv_result = (
        db.execute_async(
            "SELECT reservation_id FROM reservations_by_customer_id "
            "WHERE customer_id = %s AND book_id = %s;",
            (customer_id, book_id),
        )
        .result()
        .one()
    )

    db.execute_async(
        "UPDATE reservations_by_book_id SET reservation_date = %s "
        "WHERE book_id = %s;",
        (reservation_date, book_id),
    )
    db.execute_async(
        "UPDATE reservations_by_id SET reservation_date = %s "
        "WHERE reservation_id = %s;",
        (reservation_date, reserv_result.reservation_id),
    )

    return {
        "reservation_id": reserv_result.reservation_id,
        "reservation_date": reservation_date_obj,
    }


@app.get("/view_reservation")
async def view_reservation(book_id: int):
    future = db.execute_async(
        "SELECT * FROM reservations_by_book_id WHERE book_id = %s;", (book_id,)
    )
    result = future.result().one()

    if result:
        return {
            "reservation_id": str(result.reservation_id),
            "book_id": result.book_id,
            "customer_id": result.customer_id,
            "reservation_date": str(result.reservation_date),
        }
    else:
        raise HTTPException(400, f"No reservation for book with book_id={book_id}")


@app.get("/list_reservations")
async def list_reservations(customer_id: int | None = None):
    if customer_id is not None:
        result = (
            db.execute_async(
                "SELECT * FROM reservations_by_customer_id WHERE customer_id = %s;",
                (customer_id,),
            )
            .result()
            .all()
        )
    else:
        result = (
            db.execute_async("SELECT * FROM reservations_by_customer_id;")
            .result()
            .all()
        )
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
    return {"reservations": reservations}


@app.post("/clear")
async def clear():
    db.execute("TRUNCATE TABLE reservations_by_book_id;")
    db.execute("TRUNCATE TABLE reservations_by_id;")
    db.execute("TRUNCATE TABLE reservations_by_customer_id;")
    return Response(status_code=200, content="Successfully cleared tables")


uvicorn.run(app, port=8888, log_level="info")
