USE kestra;


DROP TABLE IF EXISTS capture_events;
CREATE TABLE capture_events (
    events_id SERIAL,
    event_title VARCHAR(256),
    event_description VARCHAR(256),
    PRIMARY KEY (events_id)
);
INSERT INTO capture_events(events_id, event_title, event_description) VALUES ( 1, 'Machine Head', 'Cool concert');
INSERT INTO capture_events(events_id, event_title, event_description) VALUES ( 2, 'Dropkick Murphys', 'Very cool concert');
INSERT INTO capture_events(events_id, event_title, event_description) VALUES ( 3, 'Pink Floyd', 'Cool');
INSERT INTO capture_events(events_id, event_title, event_description) VALUES ( 4, 'TV show', 'Some TV');
INSERT INTO capture_events(events_id, event_title, event_description) VALUES ( 5, 'Nothing', 'Boring');

DROP TABLE IF EXISTS trigger_events;
CREATE TABLE trigger_events (
    events_id SERIAL,
    event_title VARCHAR(256),
    event_description VARCHAR(256),
    PRIMARY KEY (events_id)
);
INSERT INTO trigger_events(events_id, event_title, event_description) VALUES ( 1, 'Machine Head', 'Cool concert');
INSERT INTO trigger_events(events_id, event_title, event_description) VALUES ( 2, 'Dropkick Murphys', 'Very cool concert');
INSERT INTO trigger_events(events_id, event_title, event_description) VALUES ( 3, 'Pink Floyd', 'Cool');
INSERT INTO trigger_events(events_id, event_title, event_description) VALUES ( 4, 'TV show', 'Some TV');
INSERT INTO trigger_events(events_id, event_title, event_description) VALUES ( 5, 'Nothing', 'Boring');

DROP TABLE IF EXISTS realtime_events;
CREATE TABLE realtime_events (
    events_id SERIAL,
    event_title VARCHAR(256),
    event_description VARCHAR(256),
    PRIMARY KEY (events_id)
);
INSERT INTO realtime_events(events_id, event_title, event_description) VALUES ( 1, 'Machine Head', 'Cool concert');
INSERT INTO realtime_events(events_id, event_title, event_description) VALUES ( 2, 'Dropkick Murphys', 'Very cool concert');
INSERT INTO realtime_events(events_id, event_title, event_description) VALUES ( 3, 'Pink Floyd', 'Cool');
INSERT INTO realtime_events(events_id, event_title, event_description) VALUES ( 4, 'TV show', 'Some TV');
INSERT INTO realtime_events(events_id, event_title, event_description) VALUES ( 5, 'Nothing', 'Boring');