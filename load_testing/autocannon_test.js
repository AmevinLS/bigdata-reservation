const autocannon = require("autocannon");


function setupViewRequest(request, context) {
    const book_id = Math.floor(Math.random() * 10);
    request.path = `/view_reservation?book_id=${book_id}`;
    return request;
}


async function test_view_reservation() {
    const result = await autocannon({
        url: "http://localhost:8888",
        connections: 1,
        pipelining: 1,
        duration: 0,
        amount: 100,
        // headers: {
        //     "Content-Type": "application/json"
        // },
        requests: [
            {
                method: 'GET',
                setupRequest: setupViewRequest
            }
        ]
        // requests: [
        //     (params, context, events, cb) => {
        //         params.path = generateViewParams();
        //         return cb(null, params);
        //     }
        // ]
    });
    console.log(result);
}

test_view_reservation();
