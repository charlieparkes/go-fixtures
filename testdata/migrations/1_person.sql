CREATE TABLE person (
    id SERIAL,
    first_name TEXT,
    last_name TEXT,
    address_id INT REFERENCES address
);
