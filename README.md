# Cassandra Golang Example
This repository shows basic Cassandra CRUD operations using Golang.
## Overview
We will create a keyspace according to the `scripts/cassandra.cql`:
```
CREATE KEYSPACE roster WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
USE roster;
CREATE TABLE employees (
    id UUID,
    firstname varchar,
    lastname varchar,
    age int,
    PRIMARY KEY(id)
);
CREATE INDEX ON employees(age);

CREATE TABLE messages (
    channel UUID,
    msg_id int,
    username varchar,
    content text,
    PRIMARY KEY((channel), msg_id, username)
) WITH CLUSTERING ORDER BY (msg_id DESC);
```
The keyspace will be created in Cassandra on startup. Note that we have created an index on `age` field so that we could query by `age` besides primary key.
## Usage
```bash
docker-compose up
```