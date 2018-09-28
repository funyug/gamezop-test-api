package main

import (
	"net/http"
	"github.com/gorilla/mux"
	"github.com/mediocregopher/radix.v2/redis"
	"log"
	"encoding/json"
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"math/rand"
	"time"
	"github.com/streadway/amqp"
)

type Person struct {
	ID        string   `json:"id"`
	Firstname string   `json:"firstname"`
	Lastname  string   `json:"lastname"`
}

var Conn *redis.Client
var RabbitConn *amqp.Connection
var Ch *amqp.Channel
var Q amqp.Queue
var People []Person
var DB *gorm.DB
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func main() {

	initDB()
	defer DB.Close()

	initRedis()
	defer Conn.Close()

	initRabbitmq()
	defer RabbitConn.Close()

	router := mux.NewRouter()
	router.HandleFunc("/people", CreatePerson).Methods("POST")
	router.HandleFunc("/people", GetPeople).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", router))
}

func initRabbitmq() {
	var err error
	RabbitConn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}


}

func initDB() {
	log.Println("Connecting to DB...")
	var err error
	DB, err = gorm.Open("mysql", "root:"+"@/people"+"?parseTime=true")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Connected to DB")
}

func initRedis() {
	var err error
	Conn, err = redis.Dial("tcp", "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}
}

func CreatePerson(w http.ResponseWriter, r *http.Request) {
	var person Person
	_ = json.NewDecoder(r.Body).Decode(&person)

	rand.Seed(time.Now().UnixNano())
	random_string := randSeq(10)

	resp := Conn.Cmd("HMSET", "person:" + random_string, "firstname", person.Firstname, "lastname", person.Lastname)

	if resp.Err != nil {
		log.Fatal(resp.Err)
	}

	Ch, _ := RabbitConn.Channel()

	var err error
	Q, err = Ch.QueueDeclare(
		"person", // name of the queue
		false,   // should the message be persistent? also queue will survive if the cluster gets reset
		false,   // autodelete if there's no consumers (like queues that have anonymous names, often used with fanout exchange)
		false,   // exclusive means I should get an error if any other consumer subsribes to this queue
		false,   // no-wait means I don't want RabbitMQ to wait if there's a queue successfully setup
		nil,     // arguments for more advanced configuration
	)

	if err != nil {
		log.Fatal(err)
	}


	body := "person:"+random_string
	err = Ch.Publish(
		"",     // exchange
		Q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Person added")

	json.NewEncoder(w).Encode(person)
}

func GetPeople(w http.ResponseWriter, r *http.Request) {
	var people []Person
	DB.Find(&people, Person{})
	json.NewEncoder(w).Encode(people)
}

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}