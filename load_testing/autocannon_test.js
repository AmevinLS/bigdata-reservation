const autocannon = require("autocannon");
const axios = require("axios");


function setupViewRequest(request, context) {
    const book_id = Math.floor(Math.random() * 10);
    request.path = `/view_reservation?book_id=${book_id}`;
    return request;
}

async function testViewReservation() {
    const result = await autocannon({
        url: "http://localhost:8888",
        connections: 1,
        pipelining: 1,
        duration: 0,
        amount: 10000,
        requests: [
            {
                method: 'GET',
                setupRequest: setupViewRequest
            }
        ]
    });
    console.log(result);
}


function createMakeRequests(customer_id, num_books) {
    const requests = [...Array(num_books).keys()].map(book_id => {
        return {
            // method: "GET",
            // path: `/view_reservation?book_id=${book_id}`
            method: "POST",
            path: `/make_reservation?book_id=${book_id}&customer_id=${customer_id}`
        }
    });
    return requests;
}

async function testTwoCustomerReservations(num_books) {
    const requests1 = createMakeRequests(0, num_books);
    const requests2 = createMakeRequests(1, num_books);

    const promise1 = autocannon({
        url: "http://localhost:8888",
        connections: 1,
        pipelining: 1,
        duration: 0,
        amount: num_books,
        requests: requests1
    });
    const promise2 = autocannon({
        url: "http://localhost:8888",
        connections: 1,
        pipelining: 1,
        duration: 0,
        amount: num_books,
        requests: requests2
    });

    const results1 = await promise1;
    const results2 = await promise2;
    console.log("RESULTS 1:", results1);
    console.log("RESULTS 2:", results2);
}


testTwoCustomerReservations(1000);
