from fastapi import FastAPI, HTTPException
import uvicorn

from uuid import uuid4
from datetime import datetime
from dataclasses import dataclass

from cassandra.cluster import Cluster  # type: ignore
from cassandra.query import PreparedStatement  # type: ignore


@dataclass
class PreparedStatements:
    select_reservation: PreparedStatement
    insert_reservation: PreparedStatement


RESERVATION_PER_USER_LIMIT = 100

cluster = Cluster(["127.0.0.1", "127.0.0.2"], port=9042)
db = cluster.connect("library")

prep_statements = PreparedStatements(
    select_reservation=db.prepare("SELECT * FROM reservations WHERE book_id = ?;"),
    insert_reservation=db.prepare(
        "INSERT INTO reservations (book_id, customer_id, reservation_date, reservation_id) "
        "VALUES (?, ?, ?, ?) "
        "IF NOT EXISTS;"
    ),
)


app = FastAPI()


@app.post("/make_reservation")
async def make_reservation(book_id: int, customer_id: int):
    reservation_id = uuid4()
    reservation_date = int(datetime.now().timestamp() * 1000)

    future = db.execute_async(
        prep_statements.insert_reservation,
        (book_id, customer_id, reservation_date, reservation_id),
    )

    result = future.result().one()
    if result.applied:
        return {"status": "success", "reservation_id": str(reservation_id)}
    else:
        raise HTTPException(400, "Book already reserved")


@app.get("/view_reservation")
async def view_reservation(book_id: int):
    future = db.execute_async(prep_statements.select_reservation, (book_id,))
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
async def list_reservations():
    future = db.execute_async("SELECT * FROM reservations;")
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
    return {"reservations": reservations}


@app.post("/clear")
async def clear():  # db: Session = Depends(get_db)):
    db.execute("TRUNCATE TABLE reservations;")
    db.execute("TRUNCATE TABLE customers;")


uvicorn.run(app, port=8888, log_level="info")
