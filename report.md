# Library (Book Reservation System)

*Author: Nikita Makarevich, 153989*


## Cassandra tables:

- `reservations_by_id`
    - `book_id int` 
    - `customer_id int`
    - `reservation_id uuid PRIMARY KEY`
    - `reservation_date timestamp`
- `reservations_by_book_id`
    - `book_id int PRIMARY KEY` 
    - `customer_id int`
    - `reservation_id uuid`
    - `reservation_date timestamp`
- `reservations_by_customer_id`
    - `book_id int` 
    - `customer_id int`
    - `reservation_id uuid`
    - `reservation_date timestamp`
    - `PRIMARY KEY(customer_id, book_id)`


## Architecture Description

- **Console CLI** - a Python app using Typer library
- **Backend** - a FastAPI server was used
- **Load/Stress Testing** - this was done in JS using `autocannon`


## Issues encountered
- Not allowing a single customer reserve all the books. \
*Solution*: introduce a `RESERVATION_LIMIT` - the maximum number of reservations each user can have. 
- During the initialization of Cassandra tables, an error of "incompatible schemas" occurred. \
*Solution*: check in a loop whether the schemas of all the nodes are the same - and only then continue.
- Attempts were made to better performance/throughput:
    - using asynchronous calls to Cassandra
    - using prepared statements (performance increase was negligible)