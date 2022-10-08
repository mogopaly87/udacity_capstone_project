class SqlQueries:
    
    load_readings = """
    COPY {} (year, month, tavg, tmin, tmax, prcp, wspd, pres, tsun, station_id)
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    IGNOREHEADER 1
    CSV        
        """