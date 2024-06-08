CREATE TABLE realtimetrigger_events (
    events_id NUMBER,
    event_title VARCHAR2(256),
    event_description VARCHAR2(256),
    PRIMARY KEY (events_id)
);

-- Insert statements
INSERT INTO realtimetrigger_events (events_id, event_title, event_description) VALUES (1, 'Machine Head', 'Cool concert');
INSERT INTO realtimetrigger_events (events_id, event_title, event_description) VALUES (2, 'Dropkick Murphys', 'Very cool concert');
INSERT INTO realtimetrigger_events (events_id, event_title, event_description) VALUES (3, 'Pink Floyd', 'Cool');
INSERT INTO realtimetrigger_events (events_id, event_title, event_description) VALUES (4, 'TV show', 'Some TV');
INSERT INTO realtimetrigger_events (events_id, event_title, event_description) VALUES (5, 'Nothing', 'Boring');
INSERT INTO realtimetrigger_events (events_id, event_title, event_description) VALUES (6, 'Something', 'Boring');

DELETE FROM realtimetrigger_events WHERE events_id = 5;

UPDATE realtimetrigger_events SET event_title = 'Nothing' where events_id = 6;