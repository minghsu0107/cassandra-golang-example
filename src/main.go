package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
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

func main() {
	// connect to the cluster
	cluster := gocql.NewCluster("cassandra")
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{
		NumRetries: 3,
	}
	cluster.Keyspace = "roster"
	cluster.Consistency = gocql.Quorum
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "cassandra",
		Password: "cassandra",
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
		newEmployee.Age).Exec(); err != nil {
		fmt.Println("insert error")
		log.Fatal(err)
	}

	// use select to get the employee we just entered
	var userFromDB User

	if err := session.Query("SELECT id, firstname, lastname, age FROM employees WHERE id=?", id).
		Scan(&userFromDB.ID, &userFromDB.FirstName, &userFromDB.LastName, &userFromDB.Age); err != nil {
		fmt.Println("select error")
		log.Fatal(err)
	}
	fmt.Println(userFromDB)
	var newAge int
	var newLastName string
	// Select age and lastname only
	// we have create index on age column, so we can filter by age value
	iter := session.Query("SELECT age, lastname FROM employees WHERE age = ?", 45).Iter()
	for iter.Scan(&newAge, &newLastName) {
		fmt.Println(newAge, newLastName)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	// Update James's Bond's age and first name
	if err := session.Query("UPDATE employees SET age = ?, firstname = ? WHERE id = ?", 57, "hello", id).Exec(); err != nil {
		fmt.Println("udpate error")
		log.Fatal(err)
	}

	// show the updated data
	var ageName AgeName
	if err := session.Query("SELECT firstname, age FROM employees WHERE id = ?", id).
		Scan(&ageName.FirstName, &ageName.Age); err != nil {
		fmt.Println("select error")
		log.Fatal(err)
	}
	fmt.Println(ageName)

	// Delete the employe
	if err := session.Query("DELETE FROM employees WHERE id = ?", id).Exec(); err != nil {
		fmt.Println("delete error")
		log.Fatal(err)
	}
}
