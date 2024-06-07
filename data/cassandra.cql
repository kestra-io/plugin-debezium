-- Create Keyspace
CREATE KEYSPACE IF NOT EXISTS kestra WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use Keyspace
USE kestra;

-- Drop Table If Exists
DROP TABLE IF EXISTS cassandra_types;

-- Create Table
CREATE TABLE cassandra_types (
    concert_id INT PRIMARY KEY,
    available BOOLEAN,
    a TEXT,
    b TEXT,
    c TEXT,
    d TEXT,
    play_time BIGINT,
    library_record BIGINT,
    bitn_test BLOB,
    floatn_test FLOAT,
    double_test DOUBLE,
    doublen_test DOUBLE,
    numeric_test DECIMAL,
    salary_decimal DECIMAL,
    date_type DATE,
    datetime_type TIMESTAMP,
    time_type TIME,
    timestamp_type TIMESTAMP,
    year_type INT,
    json_type TEXT,
    blob_type BLOB
) WITH cdc=true;

-- Insert Data
INSERT INTO cassandra_types (
    concert_id,
    available,
    a,
    b,
    c,
    d,
    play_time,
    library_record,
    bitn_test,
    floatn_test,
    double_test,
    doublen_test,
    numeric_test,
    salary_decimal,
    date_type,
    datetime_type,
    time_type,
    timestamp_type,
    year_type,
    json_type,
    blob_type
) VALUES (
    1,
    true,
    'four',
    'This is a varchar',
    'This is a text column data',
    NULL,
    -9223372036854775808,
    1844674407370955161,
    0x0000000000000015,
    9223372036854776000.0,
    9223372036854776000.0,
    2147483645.1234,
    5.36,
    999.99,
    '2030-12-25',
    '2050-12-31T22:59:57.150Z',
    '04:05:30',
    '2004-10-19T10:23:54.999Z',
    2025,
    '{"color":"red","value":"#f00"}',
    0xDEADBEEF
);

-- Insert Data
INSERT INTO cassandra_types (
    concert_id,
    available,
    a,
    b,
    c,
    d,
    play_time,
    library_record,
    bitn_test,
    floatn_test,
    double_test,
    doublen_test,
    numeric_test,
    salary_decimal,
    date_type,
    datetime_type,
    time_type,
    timestamp_type,
    year_type,
    json_type,
    blob_type
) VALUES (
    2,
    true,
    'four',
    'This is a varchar',
    'This is a text column data',
    NULL,
    -9223372036854775808,
    1844674407370955161,
    0x0000000000000015,
    9223372036854776000.0,
    9223372036854776000.0,
    2147483645.1234,
    5.36,
    999.99,
    '2030-12-25',
    '2050-12-31T22:59:57.150Z',
    '04:05:30',
    '2004-10-19T10:23:54.999Z',
    2025,
    '{"color":"red","value":"#f00"}',
    0xDEADBEEF
);

-- Insert Data
INSERT INTO cassandra_types (
    concert_id,
    available,
    a,
    b,
    c,
    d,
    play_time,
    library_record,
    bitn_test,
    floatn_test,
    double_test,
    doublen_test,
    numeric_test,
    salary_decimal,
    date_type,
    datetime_type,
    time_type,
    timestamp_type,
    year_type,
    json_type,
    blob_type
) VALUES (
    3,
    true,
    'four',
    'This is a varchar',
    'This is a text column data',
    NULL,
    -9223372036854775808,
    1844674407370955161,
    0x0000000000000015,
    9223372036854776000.0,
    9223372036854776000.0,
    2147483645.1234,
    5.36,
    999.99,
    '2030-12-25',
    '2050-12-31T22:59:57.150Z',
    '04:05:30',
    '2004-10-19T10:23:54.999Z',
    2025,
    '{"color":"red","value":"#f00"}',
    0xDEADBEEF
);

-- Update Data
UPDATE cassandra_types SET numeric_test = 6.36 WHERE concert_id = 2;

-- Delete Data
DELETE FROM cassandra_types WHERE concert_id = 1;

-- Create Keyspace and Table in Second Database
CREATE KEYSPACE IF NOT EXISTS second WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE second;

DROP TABLE IF EXISTS EmployeeTerritory;

CREATE TABLE EmployeeTerritory (
    employeeId INT,
    territoryId TEXT,
    PRIMARY KEY (employeeId, territoryId)
) WITH cdc=true;

-- Insert Data
INSERT INTO EmployeeTerritory (employeeId, territoryId) VALUES (1, '06897');
INSERT INTO EmployeeTerritory (employeeId, territoryId) VALUES (1, '19713');
INSERT INTO EmployeeTerritory (employeeId, territoryId) VALUES (2, '01581');
INSERT INTO EmployeeTerritory (employeeId, territoryId) VALUES (2, '01730');
INSERT INTO EmployeeTerritory (employeeId, territoryId) VALUES (2, '01833');
INSERT INTO EmployeeTerritory (employeeId, territoryId) VALUES (2, '02116');
INSERT INTO EmployeeTerritory (employeeId, territoryId) VALUES (2, '02139');

-- Delete Data
TRUNCATE EmployeeTerritory;