const autocannon = require("autocannon");
const axios = require("axios");


async function testOneCustomerSameRequest(request, num_requests) {
    const result = await autocannon({
        url: "http://localhost:8888/view_reservation?book_id=0",
        connections: 1,
        pipelining: 1,
        duration: 0,
        amount: num_requests,
        requests: [
            request
        ]
    });
    console.log(result);
}


function randomViewRequest(num_books) {
    const book_id = Math.floor(Math.random() * num_books); 
    return {
        method: "GET",
        path: `/view_reservation?book_id=${book_id}`
    };
}

function randomMakeRequest(customer_id, num_books) {
    const book_id = Math.floor(Math.random() * num_books);
    return {
        method: "POST",
        path: `/make_reservation?customer_id=${customer_id}&book_id=${book_id}`
    };
}

function randomUpdateRequest(customer_id, num_books) {
    const book_id = Math.floor(Math.random() * num_books);
    return {
        method: "POST",
        path: `/update_reservation?book_id=${book_id}&customer_id=${customer_id}`
    };
}

function randomListRequest(customer_id) {
    if (Math.random() < 0.5) {
        return {
            method: "GET",
            path: `/list_reservations?customer_id=${customer_id}`
        };
    }
    else {
        return {
            method: "GET",
            path: `/list_reservations`
        }
    }
    
}

function setupRandomRequest(request, context) {
    const request_ind = Math.floor(Math.random() * 4);
    let temp_req = null;
    switch (request_ind) {
        case 0:
            temp_req = randomViewRequest(context.num_books);
            break;
        case 1:
            temp_req = randomMakeRequest(context.customer_id, context.num_books);
            break;
        case 2:
            temp_req = randomUpdateRequest(context.customer_id, context.num_books);
            break;
        case 3:
            temp_req = randomListRequest(context.customer_id);
            break;
        default:
            throw Error("This isn't supposed to happen");
    }
    request.method = temp_req.method;
    request.path = temp_req.path;
    return request;
}

async function testTwoCustomersRandomRequests(num_books, num_requests_each) {
    console.log("Started two-customer random requests stress test...");
    const promise1 = autocannon({
        url: "http://localhost:8888",
        connections: 1,
        pipelining: 1,
        duration: 0,
        amount: num_requests_each,
        initialContext: {
            num_books: num_books,
            customer_id: 0
        },
        requests: [
            {
                setupRequest: setupRandomRequest
            }
        ]
    });
    const promise2 = autocannon({
        url: "http://localhost:8888",
        connections: 1,
        pipelining: 1,
        duration: 0,
        amount: num_requests_each,
        initialContext: {
            num_books: num_books,
            customer_id: 1
        },
        requests: [
            {
                setupRequest: setupRandomRequest
            }
        ]
    });

    const results1 = await promise1;
    const results2 = await promise2;
    console.log("RESULTS 1:", results1);
    console.log("RESULTS 2:", results2);
}


function createConsecutiveMakeRequests(customer_id, num_books) {
    const requests = [...Array(num_books).keys()].map(book_id => {
        return {
            method: "POST",
            path: `/make_reservation?book_id=${book_id}&customer_id=${customer_id}`
        }
    });
    return requests;
}

async function testTwoCustomersReservations(num_books) {
    const requests1 = createConsecutiveMakeRequests(0, num_books);
    const requests2 = createConsecutiveMakeRequests(1, num_books);

    console.log("Started two-customer reservations stress test...");
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


const num_books = 1000;

// Stress test 1
const num_requests = 1000;
testOneCustomerSameRequest(
    {
        method: "GET",
        path: "/view_reservation?book_id=0"
    },
    num_requests
)

// Stress test 2
// const num_requests_each = 1000;
// testTwoCustomersRandomRequests(num_books, num_requests_each);

// Stress test 3
// testTwoCustomersReservations(num_books);
