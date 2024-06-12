CREATE TABLE events (
    events_id NUMBER,
    event_title VARCHAR2(256),
    event_description VARCHAR2(256),
    PRIMARY KEY (events_id)
);

-- Insert statements
INSERT INTO events (events_id, event_title, event_description) VALUES (1, 'Machine Head', 'Cool concert');
INSERT INTO events (events_id, event_title, event_description) VALUES (2, 'Dropkick Murphys', 'Very cool concert');
INSERT INTO events (events_id, event_title, event_description) VALUES (3, 'Pink Floyd', 'Cool');
INSERT INTO events (events_id, event_title, event_description) VALUES (4, 'TV show', 'Some TV');
INSERT INTO events (events_id, event_title, event_description) VALUES (5, 'Nothing', 'Boring');
INSERT INTO events (events_id, event_title, event_description) VALUES (6, 'Something', 'Boring');

DELETE FROM events WHERE events_id = 6;