CREATE TABLE public.staging_readings (
    year int4,
    month int4,
    tavg numeric,
    tmin numeric,
    tmax numeric,
    prcp numeric,
    wspd numeric,
    pres numeric,
    tsun int4,
    station_id VARCHAR(256)
)   DISTKEY(year) SORTKEY(year);


CREATE TABLE public.staging_station (
    id varchar(256),
    english_name varchar(256),
    country varchar(256),
    region varchar(256),
    latitude varchar(256),
    longitude varchar(256),
    elevation varchar(256),
    timezone varchar(256),
    "start" varchar(256),
    "end" varchar(256)
);