-- Drop the events table if it exists
DROP TABLE IF EXISTS DB2INST1.events;

-- Create the 'events' table
CREATE TABLE DB2INST1.events (
    events_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_title VARCHAR(256),
    event_description VARCHAR(256)
);

-- Insert data into the 'events' table
INSERT INTO events (event_title, event_description) VALUES ('Machine Head', 'Cool concert');
INSERT INTO events (event_title, event_description) VALUES ('Dropkick Murphys', 'Very cool concert');
INSERT INTO events (event_title, event_description) VALUES ('Pink Floyd', 'Cool');
INSERT INTO events (event_title, event_description) VALUES ('TV show', 'Some TV');
INSERT INTO events (event_title, event_description) VALUES ('Nothing', 'Boring');