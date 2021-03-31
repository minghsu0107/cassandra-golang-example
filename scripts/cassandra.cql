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
