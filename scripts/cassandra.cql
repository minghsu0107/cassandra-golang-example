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
