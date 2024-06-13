# BigData Reservation System (Library)

This Library app is a FastAPI server, which connects to a Cassandra database. There is also a CLI app provided.

This is a 'library' system, where you can:
- List either all books in the library, or only the available ones
- Reserve a book for a customer by `book_id` and `customer_id`
- View a reservation by it's `reservation_id` (which is automatically generated and returned after reserving a book)
- Update a reservation (re-reserve it for the same customer)
- List all reservations made (optionally, filtered by `customer_id`) 


## Setup

1. **Start Cassandra node containers**
    ```bash
    docker-compose up -d
    ```
2. **Initialize Cassandra tables**
    ```bash
    python library_app/init_cassandra.py
    ```
    *(This can take some time)*
3. **Start FastAPI server**
    ```bash
    python library_app/server.py
    ```


## Library Usage
    
- **You can use the CLI app**
    ```bash
    python library_app/app.py --help
    ```
- **You can make calls to the server directly** \
    Go to http://localhost:8888/docs to familiarize yourself with the available endpoints


## Stress Tests
1. **Install dependencies**
    ```bash
    npm install
    ```
2. **Run a stress test**
    ```bash
    node autocannon_test.js STRESS_TEST_NUM
    ```
    where `STRESS_TEST_NUM` $\in \{1,2,3\}$ is the number of stress test 