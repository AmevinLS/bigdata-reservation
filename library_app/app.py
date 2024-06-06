import typer
from rich.style import Style
from rich.console import Console
import pandas as pd  # type: ignore

from typing import Optional
import requests  # type: ignore

SERVER_URL = "http://localhost:8888"
app = typer.Typer()

SUCCESS_STYLE = Style(color="green", bold=True)
ERROR_STYLE = Style(color="red", bold=True)
console = Console()


@app.command()
def list_books(only_available: bool = False):
    response = requests.get(
        f"{SERVER_URL}/books",
        params={"only_available": only_available},
    )
    if response.ok:
        books_df = pd.DataFrame(response.json()["books"])
        qualifier = "only available" if only_available else "all"
        console.print(
            f"Here is the list of ({qualifier}) books:",
            style=SUCCESS_STYLE,
        )
        console.print(books_df)
    else:
        console.print(response.json()["detail"], style=ERROR_STYLE)


@app.command()
def make_reservation(customer_id: int, book_id: int):
    response = requests.post(
        f"{SERVER_URL}/make_reservation",
        params={"customer_id": customer_id, "book_id": book_id},
    )
    if response.ok:
        reservation_id = response.json()["reservation_id"]
        console.print(
            f"Created reservation with 'reservation_id' = {reservation_id}",
            style=SUCCESS_STYLE,
        )
    else:
        console.print(response.json()["detail"], style=ERROR_STYLE)


@app.command()
def view_reservation(book_id: int):
    response = requests.get(
        f"{SERVER_URL}/view_reservation",
        params={"book_id": book_id},
    )
    if response.ok:
        console.print("Successfully retrieved reservation", style=SUCCESS_STYLE)
        console.print(response.json())
    else:
        console.print(response.json()["detail"], style=ERROR_STYLE)


@app.command()
def update_reservation(customer_id: int, book_id: int):
    response = requests.post(
        f"{SERVER_URL}/update_reservation",
        params={"customer_id": customer_id, "book_id": book_id},
    )
    if response.ok:
        console.print("Successfully updated reservation", style=SUCCESS_STYLE)
        console.print(response.json())
    else:
        console.print(response.json()["detail"], style=ERROR_STYLE)


@app.command()
def list_reservations(customer_id: Optional[int] = None):
    params = {"customer_id": customer_id} if customer_id is not None else {}
    response = requests.get(f"{SERVER_URL}/list_reservations", params=params)
    if response.ok:
        df = pd.DataFrame(response.json()["reservations"])
        console.print("Here are the reservations:", style=SUCCESS_STYLE)
        console.print(df)
    else:
        console.print(response.json()["detail"], style=ERROR_STYLE)


if __name__ == "__main__":
    app()
