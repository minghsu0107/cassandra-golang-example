package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocql/gocql"
)

var (
	cassandraHost     string = os.Getenv("CASSANDRA_HOST")
	cassandraUser     string = os.Getenv("CASSANDRA_USER")
	cassandraPassword string = os.Getenv("CASSANDRA_PASSWORD")
)

// User desscribes an employee entity.
// We will be using this to demonstrate Cassandra create, read, update, and delete flows
type User struct {
	ID        gocql.UUID
	FirstName string
	LastName  string
	Age       int
}

// AgeName is the update schema
type AgeName struct {
	FirstName string
	Age       int
}

// Message is message entity.
// We will be using this to demonstrate page state
type Message struct {
	Channel  gocql.UUID
	MsgID    int
	Username string
	Content  string
}

func main() {
	// connect to the cluster
	cluster := gocql.NewCluster(cassandraHost)
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{
		NumRetries: 3,
	}
	cluster.Keyspace = "roster"
	cluster.Consistency = gocql.Quorum
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cassandraUser,
		Password: cassandraPassword,
	}
	var session *gocql.Session
	var err error
	for {
		session, err = cluster.CreateSession()
		if err == nil {
			break
		}
		log.Printf("CreateSession: %v", err)
		time.Sleep(time.Second)
	}
	log.Printf("Connected OK\n")
	defer session.Close()

	// generate a unique id for the employee
	id := gocql.TimeUUID()
	// create the employee in memory
	newEmployee := User{
		ID:        id,
		FirstName: "James",
		LastName:  "Bond",
		Age:       45,
	}

	// insert the employee
	if err := session.Query("INSERT INTO employees (id, firstname, lastname, age) VALUES (?, ?, ?, ?)",
		newEmployee.ID,
		newEmployee.FirstName,
		newEmployee.LastName,
		newEmployee.Age).WithContext(context.Background()).Exec(); err != nil {
		fmt.Println("insert error")
		log.Fatal(err)
	}

	// use select to get the employee we just entered
	var userFromDB User

	if err := session.Query("SELECT id, firstname, lastname, age FROM employees WHERE id=?", id).
		WithContext(context.Background()).
		Scan(&userFromDB.ID, &userFromDB.FirstName, &userFromDB.LastName, &userFromDB.Age); err != nil {
		fmt.Println("select error")
		log.Fatal(err)
	}
	fmt.Println(userFromDB)
	var newAge int
	var newLastName string
	// Select age and lastname only
	// we have create index on age column, so we can filter by age value
	iter := session.Query("SELECT age, lastname FROM employees WHERE age = ?", 45).
		WithContext(context.Background()).Iter()
	for iter.Scan(&newAge, &newLastName) {
		fmt.Println(newAge, newLastName)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	// Update James's Bond's age and first name
	if err := session.Query("UPDATE employees SET age = ?, firstname = ? WHERE id = ?", 57, "hello", id).
		WithContext(context.Background()).Exec(); err != nil {
		fmt.Println("udpate error")
		log.Fatal(err)
	}

	// show the updated data
	var ageName AgeName
	if err := session.Query("SELECT firstname, age FROM employees WHERE id = ?", id).
		WithContext(context.Background()).
		Scan(&ageName.FirstName, &ageName.Age); err != nil {
		fmt.Println("select error")
		log.Fatal(err)
	}
	fmt.Println(ageName)

	// Delete the employee
	if err := session.Query("DELETE FROM employees WHERE id = ?", id).
		WithContext(context.Background()).Exec(); err != nil {
		fmt.Println("delete error")
		log.Fatal(err)
	}

	channel := gocql.TimeUUID()
	messages := []Message{
		{
			Channel:  channel,
			MsgID:    1,
			Username: "ming",
			Content:  "one",
		},
		{
			Channel:  channel,
			MsgID:    2,
			Username: "ming",
			Content:  "two",
		},
		{
			Channel:  channel,
			MsgID:    3,
			Username: "ming",
			Content:  "three",
		},
		{
			Channel:  channel,
			MsgID:    4,
			Username: "sam",
			Content:  "four",
		},
		{
			Channel:  channel,
			MsgID:    5,
			Username: "sam",
			Content:  "five",
		},
	}
	// insert employees
	for _, message := range messages {
		if err := session.Query("INSERT INTO messages (channel, username, msg_id, content) VALUES (?, ?, ?, ?)",
			message.Channel,
			message.Username,
			message.MsgID,
			message.Content,
		).WithContext(context.Background()).Exec(); err != nil {
			fmt.Println("insert error")
			log.Fatal(err)
		}
	}

	// page state
	// use larger values in production (default is 5000) for performance reasons
	var pageState []byte
	size := 2
	for {
		iter := session.Query(`SELECT username, content FROM messages WHERE channel = ?`, channel).
			WithContext(context.Background()).PageSize(size).PageState(pageState).Iter()
		nextPageState := iter.PageState()
		scanner := iter.Scanner()
		for scanner.Next() {
			var (
				username string
				content  string
			)
			err = scanner.Scan(&username, &content)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(username, content)
		}
		err = scanner.Err()
		if err != nil {
			log.Fatal(err)
		}
		if len(nextPageState) == 0 {
			break
		}
		pageState = nextPageState
	}

	// insert null value
	if err := session.Query("INSERT INTO messages (channel, username, msg_id, content) VALUES (?, ?, ?, ?)",
		channel,
		"ming",
		6,
		nil,
	).WithContext(context.Background()).Exec(); err != nil {
		fmt.Println("insert error")
		log.Fatal(err)
	}

	scanner := session.Query(`SELECT msg_id, content FROM messages WHERE channel = ?`, channel).WithContext(context.Background()).Iter().Scanner()
	for scanner.Next() {
		var (
			msgID   int
			content *string
		)
		err := scanner.Scan(&msgID, &content)
		if err != nil {
			log.Fatal(err)
		}
		if content != nil {
			fmt.Printf("msgID: %v, content: %q\n", msgID, *content)
		} else {
			fmt.Printf("msgID: %v, Content is null\n", msgID)
		}

	}
	err = scanner.Err()
	if err != nil {
		log.Fatal(err)
	}
}
