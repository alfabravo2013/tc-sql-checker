CREATE DATABASE sandbox;
CREATE TABLE testdb(id int, name varchar(255), created timestamp NOT NULL DEFAULT now());
INSERT INTO testdb (id, name) 
VALUES 
    (1, 'bub'),
    (2, 'bob'),
    (3, 'bab'),
    (4, 'beb'),
    (5, 'bib');
