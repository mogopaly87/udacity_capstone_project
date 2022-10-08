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