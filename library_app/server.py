from fastapi import FastAPI, HTTPException, Response
import uvicorn

from uuid import uuid4
from datetime import datetime

from library import LibraryWrapper


RESERVATION_PER_USER_LIMIT = 10

library = LibraryWrapper()

app = FastAPI()


@app.post("/make_reservation")
async def make_reservation(book_id: int, customer_id: int):
    reservation_id = uuid4()
    reservation_date_obj = datetime.now()
    reservation_date = int(reservation_date_obj.timestamp() * 1000)

    count_result = await library.get_reservation_count(customer_id)
    if count_result >= RESERVATION_PER_USER_LIMIT:
        raise HTTPException(429, "Reservation limit reached")

    # Insert LWT
    applied = await library.insert_reservation(
        book_id, customer_id, reservation_id, reservation_date
    )
    if not applied:
        raise HTTPException(409, "Book already reserved.")

    return {"status": "success", "reservation_id": str(reservation_id)}


@app.post("/update_reservation")
async def update_reservation(book_id: int, customer_id: int):
    reservation_date_obj = datetime.now()
    reservation_date = int(reservation_date_obj.timestamp() * 1000)

    applied = await library.update_reservation(reservation_date, book_id, customer_id)
    if not applied:
        raise HTTPException(
            400, "Cannot update reservation with earlier 'reservation_date'"
        )

    return {
        "reservation_date": reservation_date_obj,
    }


@app.get("/view_reservation")
async def view_reservation(book_id: int):
    result = await library.get_reservation(book_id)
    if result is None:
        raise HTTPException(404, f"No reservation for book with book_id={book_id}")
    return result


@app.get("/list_reservations")
async def list_reservations(customer_id: int | None = None):
    reservations = await library.get_reservations(customer_id)
    return {"reservations": reservations}


@app.get("/books")
async def get_books(only_available: bool = False):
    books = await library.get_books(only_available)
    return {"books": books}


@app.post("/clear")
async def clear():
    await library.clear_reservations()
    return Response(status_code=200, content="Successfully cleared tables")


uvicorn.run(app, port=8888, log_level="info")
