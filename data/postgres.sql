DROP TABLE IF EXISTS pl_store_distribute;

CREATE TABLE pl_store_distribute (
    id SERIAL NOT NULL,
    year_month NUMERIC(6, 0) NOT NULL,
    store_code NUMERIC NOT NULL,
    is_building NUMERIC(1, 0) DEFAULT 0,
    is_cgm_visible NUMERIC(1, 0) DEFAULT 0,
    is_validate NUMERIC(1, 0) DEFAULT 0,
    is_publish NUMERIC(1, 0) DEFAULT 0,
    update_date TIMESTAMP(0) WITHOUT TIME ZONE DEFAULT now(),
    update_ldap CHARACTER VARYING(80),
    CONSTRAINT store_distribute_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS pgsql_types;

DROP TYPE IF EXISTS inventory_item;

/* Composite Types */
CREATE TYPE INVENTORY_ITEM AS (
    name TEXT,
    supplier_id INTEGER,
    price NUMERIC
);


CREATE TABLE pgsql_types (
    concert_id SERIAL NOT NULL,
    available BOOLEAN NOT NULL,
    a CHAR(4) NOT NULL,
    b VARCHAR(30) NOT NULL,
    c TEXT NOT NULL,
    d VARCHAR(10),
    play_time SMALLINT NOT NULL,
    library_record BIGINT NOT NULL,
    -- money_type money not null,
    floatn_test FLOAT8 NOT NULL,
    double_test DOUBLE PRECISION NOT NULL,
    real_test REAL NOT NULL,
    numeric_test NUMERIC NOT NULL,
    date_type DATE NOT NULL,
    time_type TIME NOT NULL,
    timez_type TIME WITH TIME ZONE NOT NULL,
    timestamp_type TIMESTAMP NOT NULL,
    timestampz_type TIMESTAMP WITH TIME ZONE NOT NULL,
    interval_type INTERVAL NOT NULL,
    pay_by_quarter INTEGER[] NOT NULL,
    schedule TEXT[][] NOT NULL,
    json_type JSON NOT NULL,
    item INVENTORY_ITEM NOT NULL,
    blob_type BYTEA NOT NULL,
    PRIMARY KEY (concert_id)
);


-- Insert
INSERT INTO pgsql_types
(
    concert_id,
    available,
    a,
    b,
    c,
    d,
    play_time,
    library_record,
    -- money_type,
    floatn_test,
    double_test,
    real_test,
    numeric_test,
    date_type,
    time_type,
    timez_type,
    timestamp_type,
    timestampz_type,
    interval_type,
    pay_by_quarter,
    schedule,
    json_type,
    item,
    blob_type
)
VALUES
(
    DEFAULT,
    TRUE,
    'four',
    'This is a varchar',
    'This is a text column data',
    NULL,
    32767,
    9223372036854775807,
    -- 999999.999,
    9223372036854776000,
    9223372036854776000,
    9223372036854776000,
    2147483645.1234,
    '2030-12-25',
    '04:05:30',
    '04:05:06 PST',
    '2004-10-19 10:23:54.999999',
    '2004-10-19 10:23:54.250+04',
    '10 years 4 months 5 days 10 seconds',
    '{100,200,300}',
    '{{meeting,lunch},{training,presentation}}',
    '{
      "color": "red",
      "value": "#f00"
    }',
    Row('fuzzy dice', 42, 1.99),
    '\xDEADBEEF'
);


-- Insert
INSERT INTO pgsql_types
(
    concert_id,
    available,
    a,
    b,
    c,
    d,
    play_time,
    library_record,
    -- money_type,
    floatn_test,
    double_test,
    real_test,
    numeric_test,
    date_type,
    time_type,
    timez_type,
    timestamp_type,
    timestampz_type,
    interval_type,
    pay_by_quarter,
    schedule,
    json_type,
    item,
    blob_type
)
VALUES
(
    DEFAULT,
    TRUE,
    'four',
    'This is a varchar',
    'This is a text column data',
    NULL,
    32767,
    9223372036854775807,
    -- 999999.999,
    9223372036854776000,
    9223372036854776000,
    9223372036854776000,
    2147483645.1234,
    '2030-12-25',
    '04:05:30',
    '04:05:06 PST',
    '2004-10-19 10:23:54.999999',
    '2004-10-19 10:23:54.250+04',
    '10 years 4 months 5 days 10 seconds',
    '{100,200,300}',
    '{{meeting,lunch},{training,presentation}}',
    '{
      "color": "red",
      "value": "#f00"
    }',
    Row('fuzzy dice', 42, 1.99),
    '\xDEADBEEF'
);


-- Insert
INSERT INTO pgsql_types
(
    concert_id,
    available,
    a,
    b,
    c,
    d,
    play_time,
    library_record,
    -- money_type,
    floatn_test,
    double_test,
    real_test,
    numeric_test,
    date_type,
    time_type,
    timez_type,
    timestamp_type,
    timestampz_type,
    interval_type,
    pay_by_quarter,
    schedule,
    json_type,
    item,
    blob_type
)
VALUES
(
    DEFAULT,
    TRUE,
    'four',
    'This is a varchar',
    'This is a text column data',
    NULL,
    32767,
    9223372036854775807,
    -- 999999.999,
    9223372036854776000,
    9223372036854776000,
    9223372036854776000,
    2147483645.1234,
    '2030-12-25',
    '04:05:30',
    '04:05:06 PST',
    '2004-10-19 10:23:54.999999',
    '2004-10-19 10:23:54.250+04',
    '10 years 4 months 5 days 10 seconds',
    '{100,200,300}',
    '{{meeting,lunch},{training,presentation}}',
    '{
      "color": "red",
      "value": "#f00"
    }',
    Row('fuzzy dice', 42, 1.99),
    '\xDEADBEEF'
);

-- Update
UPDATE pgsql_types
SET numeric_test = 6.36
WHERE concert_id = 2;

-- Delete
DELETE
FROM pgsql_types
WHERE concert_id = 1;

