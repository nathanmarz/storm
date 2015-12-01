CREATE TABLE temperature (
    weatherstation_id   TEXT,
    event_time          TIMEUUID,
    temperature         TEXT,
    PRIMARY KEY(weatherstation_id, event_time)
)

