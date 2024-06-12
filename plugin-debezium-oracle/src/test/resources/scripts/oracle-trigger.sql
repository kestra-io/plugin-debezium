CREATE TABLE trigger_events (
    events_id NUMBER,
    event_title VARCHAR2(256),
    event_description VARCHAR2(256),
    PRIMARY KEY (events_id)
);

-- Insert statements
INSERT INTO trigger_events (events_id, event_title, event_description) VALUES (1, 'Machine Head', 'Cool concert');
INSERT INTO trigger_events (events_id, event_title, event_description) VALUES (2, 'Dropkick Murphys', 'Very cool concert');
INSERT INTO trigger_events (events_id, event_title, event_description) VALUES (3, 'Pink Floyd', 'Cool');
INSERT INTO trigger_events (events_id, event_title, event_description) VALUES (4, 'TV show', 'Some TV');
INSERT INTO trigger_events (events_id, event_title, event_description) VALUES (5, 'Nothing', 'Boring');
INSERT INTO trigger_events (events_id, event_title, event_description) VALUES (6, 'Something', 'Boring');

DELETE FROM trigger_events WHERE events_id = 5;

UPDATE trigger_events SET event_title = 'Nothing' where events_id = 6;