import csv
import functools
import collections
import numpy as np
import textwrap
import cassandra
import cassandra.cluster
import flight_data

class FlightData:
    """ Flight data manager (stored cassandra tables & originally in csv files)."""

    def __init__(self, keyspace):
        self._cluster = cassandra.cluster.Cluster()
        self._session = self._cluster.connect(keyspace)

    def _insert_query_by_datetime(self, flight):
        
        """ Returns a query to insert a flight's data in the flight_by_datetime CQL table.

        Parameters
        ------------
        flight:
                tuple containing the following field values of the flight to insert : 
                "Year", "Month", "Day", "DayOfWeek", 
                "CRSDepHour", "CRSDepMin", "CRSArrHour", "CRSArrMin",
                "DepDelay", "ArrDelay", "CarrierDelay", "WeatherDelay", "NASDelay", 
                "SecurityDelay", "LateAircraftDelay", 
                "TailNum", "MFRYear","FlightNum"

        """

        query = textwrap.dedent(
            f"""
            INSERT INTO flight_by_datetime(
            year,
            month,
            day,
            dep_hour,
            dep_min,
            arr_hour,
            arr_min,
            depdelay,
            arrdelay,
            carrierdel,
            weatherdel,
            NASdel,
            securitydel,
            lateACdel,
            flightnum 
            )
            VALUES
            (
            {flight.Year},
            {flight.Month},
            {flight.Day},
            {flight.CRSDepHour},
            {flight.CRSDepMin},
            {flight.CRSArrHour},
            {flight.CRSArrMin},
            {flight.DepDelay},
            {flight.ArrDelay},
            {flight.CarrierDelay},
            {flight.WeatherDelay},
            {flight.NASDelay},
            {flight.SecurityDelay},
            {flight.LateAircraftDelay},
            {flight.FlightNum}
            );
            """
        )
        return query
    
    def _insert_query_by_dayofweek(self, flight):
        
        """ Returns a query to insert a flight's data in the flight_by_dayofweek CQL table.

        Parameters
        ------------
        flight:
                tuple containing the following field values of the flight to insert : 
                "Year", "Month", "Day", "DayOfWeek", 
                "CRSDepHour", "CRSDepMin", "CRSArrHour", "CRSArrMin",
                "DepDelay", "ArrDelay", "CarrierDelay", "WeatherDelay", "NASDelay", 
                "SecurityDelay", "LateAircraftDelay", 
                "TailNum", "MFRYear","FlightNum"

        """
        query = textwrap.dedent(
            f"""
            INSERT INTO flight_by_dayofweek(
            year,
            month,
            day,
            dayofweek,
            dep_hour,
            dep_min,
            arr_hour,
            arr_min,
            depdelay,
            arrdelay,
            carrierdel,
            weatherdel,
            NASdel,
            securitydel,
            lateACdel,
            flightnum 
            )
            VALUES
            (
            {flight.Year},
            {flight.Month},
            {flight.Day},
            {flight.DayOfWeek},
            {flight.CRSDepHour},
            {flight.CRSDepMin},
            {flight.CRSArrHour},
            {flight.CRSArrMin},
            {flight.DepDelay},
            {flight.ArrDelay},
            {flight.CarrierDelay},
            {flight.WeatherDelay},
            {flight.NASDelay},
            {flight.SecurityDelay},
            {flight.LateAircraftDelay},
            {flight.FlightNum}
            );
            """
        )
        return query

    def _insert_query_by_dephour(self, flight):
        """ Returns a query to insert a flight's data in the flight_by_dephour CQL table.

        Parameters
        ------------
        flight:
                tuple containing the following field values of the flight to insert : 
                "Year", "Month", "Day", "DayOfWeek", 
                "CRSDepHour", "CRSDepMin", "CRSArrHour", "CRSArrMin",
                "DepDelay", "ArrDelay", "CarrierDelay", "WeatherDelay", "NASDelay", 
                "SecurityDelay", "LateAircraftDelay", 
                "TailNum", "MFRYear","FlightNum"

        """
        query = textwrap.dedent(
            f"""
            INSERT INTO flight_by_dephour(
            year,
            month,
            day,
            dep_hour,
            dep_min,
            arr_hour,
            arr_min,
            depdelay,
            arrdelay,
            carrierdel,
            weatherdel,
            NASdel,
            securitydel,
            lateACdel,
            flightnum 
            )
            VALUES
            (
            {flight.Year},
            {flight.Month},
            {flight.Day},
            {flight.CRSDepHour},
            {flight.CRSDepMin},
            {flight.CRSArrHour},
            {flight.CRSArrMin},
            {flight.DepDelay},
            {flight.ArrDelay},
            {flight.CarrierDelay},
            {flight.WeatherDelay},
            {flight.NASDelay},
            {flight.SecurityDelay},
            {flight.LateAircraftDelay},
            {flight.FlightNum}
            );
            """
        )
        return query

    def insert_csv(self, fname, planeDict, limit=None):

        """ Inserts csv flight data in all CQL tables.

        Parameters
        ------------
        fname:
               name of CSV file to read.
        planeDict:
               dictionary containing Plane data.  
        limit:
               max number of inserted elements.
        """

        stream = flight_data.read_flight_csv(fname, planeDict)
        
        INSERT_Q = (
            self._insert_query_by_datetime, 
            self._insert_query_by_dayofweek,
            self._insert_query_by_dephour
        )

        if limit is not None:
            stream = flight_data.limiter(stream, limit)
        for flight in stream:
            for q in INSERT_Q:
                query = q(flight)
                self._session.execute(query)

    
    def get_flight_by_dow(self, dow, yow):
        
        """ Yields results of a query from flight_by_dayofweek table.

        Parameters
        ------------
        dow:
               day of week.
        yow:
               year of the searched weeks.  
        
        """

        query = textwrap.dedent(
            f"""
            SELECT
            year, month, day, dayofweek, dep_hour, dep_min, arr_hour, arr_min,
            depdelay, arrdelay, carrierdel, weatherdel, NASdel,
            securitydel, lateACdel, flightnum 
            FROM
            flight_by_dayofweek
            WHERE
                year = {yow}
                AND
                dayofweek = {dow}
            ;
            """
            )
        for r in self._session.execute(query):
                yield flight_data.Flight(r.year, r.month, r.day, 
                        r.dayofweek, r.dep_hour, r.dep_min, 
                        r.arr_hour, r.arr_min,
                        r.depdelay, r.arrdelay, 
                        np.nan, r.weatherdel, np.nan, 
                        np.nan, np.nan, 
                        0, 0, r.flightnum)

    def get_flight_by_datetime(self, year, month, day):
        
        """ Yields results of a query from flight_by_datetime table using year/month/day.

        Parameters
        ------------
        year:
               year of searched flights.
        month:
               month number of searched flights. 
        day:
               day of month of searched flights. 
        
        """

        query = textwrap.dedent(
            f"""
            SELECT
            year, month, day, dep_hour, dep_min, arr_hour, arr_min,
            depdelay, arrdelay, carrierdel, weatherdel, NASdel,
            securitydel, lateACdel, flightnum 
            FROM
            flight_by_datetime
            WHERE
                year = {year}
                AND
                month = {month}
                AND
                day = {day}
            ;
            """
            )

        for r in self._session.execute(query):            
                yield flight_data.Flight(r.year, r.month, r.day, 
                        9, r.dep_hour, r.dep_min, 
                        r.arr_hour, r.arr_min,
                        r.depdelay, r.arrdelay, 
                        np.nan, r.weatherdel, np.nan, 
                        np.nan, np.nan, 
                        0, 0, r.flightnum)   

    def get_flights_by_month(self, month):
        
        """ Yields results of a query from flight_by_datetime table using only month.

        Parameters
        ------------
        month:
               month number of searched flights. 
        
        """

        for year in range(1987, 2008):  #years for which data is available
            for day in range(1,32):
                for x in self.get_flight_by_datetime(year, month, day):
                    yield x

    
    def get_flight_by_dephour(self, hour, minute, year):
        
        """ Yields results of a query from flight_by_dephour table using departure year/hour/minute.

        Parameters
        ------------
        hour:
               departure hour of searched flights.
        minute:
               departure minutes of searched flights. 
        year:
               departure year of searched flights. 
        
        """

        query = textwrap.dedent(
            f"""
            SELECT
            year, month, day, dep_hour, dep_min, 
            arr_hour, arr_min,
            depdelay, arrdelay, 
            carrierdel, weatherdel, NASdel,
            securitydel, lateACdel, flightnum 
            FROM
            flight_by_dephour
            WHERE
                dep_hour = {hour}
                AND
                dep_min = {minute}
                AND
                year = {year}
            ;
            """
            )

        for r in self._session.execute(query):
                yield flight_data.Flight(r.year, r.month, r.day, 
                        9, r.dep_hour, r.dep_min, 
                        r.arr_hour, r.arr_min,
                        r.depdelay, r.arrdelay, 
                        np.nan, r.weatherdel, np.nan, 
                        np.nan, np.nan, 
                        0, 0, r.flightnum) 

    
    
    def get_flights_by_hour(self, hour):
        
        """ Yields results of a query from flight_by_dephour table using departure hour.

        Parameters
        ------------
        hour:
               departure hour of searched flights.
        
        """

        for year in range(1987, 2008):  #years for which data is available
            for minute in range(60):
                for x in self.get_flight_by_dephour(hour, minute, year):
                    yield x