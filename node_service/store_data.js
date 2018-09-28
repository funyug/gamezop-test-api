var redis = require("redis"),
    client = redis.createClient();

var amqp = require('amqplib/callback_api');
var mysql = require('mysql');

var conn = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "",
    database:"people"
});


amqp.connect('amqp://localhost', function(err, conn) {
    conn.createChannel(function(err, ch) {
        var q = 'person';
        ch.assertQueue(q, {durable: false});
        ch.consume(q, function(msg) {
            data = msg.content.toString();
            get_data(data);
            console.log(" [x] Received %s", data);
        }, {noAck: true});
    });
});

function get_data(key) {
    client.hgetall(key, function(err, reply) {
        conn.query('INSERT INTO people set ?', reply, function(err, result) {
            if(err !== null) {
                console.log(err);
            } else {
                console.log("Data inserted to db");
                client.del(key,function(err,reply) {
                    if(err !== null) {
                        console.log(err);
                    }
                    console.log("Data deleted from redis");
                })
            }
        });
    });
}