import threading
import requests  # type: ignore
from collections import Counter
import time


def reserve_all_books(customer_id: int, num_books: int):
    codes_counter: Counter[int] = Counter()
    for book_id in range(num_books):
        response = requests.post(
            "http://localhost:8888/make_reservation",
            params={"customer_id": customer_id, "book_id": book_id},
        )
        codes_counter[response.status_code] += 1
        if response.ok:
            time.sleep(0.2)
    print(f"Customer {customer_id} status codes:\n{codes_counter}", end="\n\n")
    return codes_counter


if __name__ == "__main__":
    num_books = 20
    threads = [
        threading.Thread(target=reserve_all_books, args=(0, num_books)),
        threading.Thread(target=reserve_all_books, args=(1, num_books)),
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
