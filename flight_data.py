import csv
import time
import collections
import textwrap
import numpy as np


# LIMITERS

def limiter(generator, limit): 
    """Returns generator with limited number of elements (first elements).
    
    Parameters
    ------------
    generator:
            generator to limit.
    limit:
            max number of generated elements.

    """

    return (
    data for _, data in zip(range(limit), generator))

## READ CSV PRINCIPAL

Flight = collections.namedtuple(
    "Flight", ("Year", "Month", "Day", "DayOfWeek", 
               "CRSDepHour", "CRSDepMin",
               "CRSArrHour", "CRSArrMin",
               "DepDelay", "ArrDelay", 
               "CarrierDelay", "WeatherDelay", "NASDelay", 
               "SecurityDelay", "LateAircraftDelay", 
               "TailNum", "MFRYear",
               "FlightNum"
               )
)


def read_flight_csv(file, planeDict):
    """Creates generator from flight csv file.
    
    Parameters
    ------------
    file:
            name of CSV file to read.
    planeDict:
            Dictionary containing Plane data.

    """
    with open(file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                year = int(row["Year"])
                month = int(row["Month"])
                day = int(row["DayofMonth"])
                dow = int(row["DayOfWeek"])
                
                dep_hour = int(row["CRSDepTime"][:2])
                dep_min =int(row["CRSDepTime"][2:])
                
                arr_hour = int(row["CRSArrTime"][:2])
                arr_min = int(row["CRSArrTime"][2:])
                
                depdelay = int(row["DepDelay"])
                arrdelay = int(row["ArrDelay"])
                carrierdel = int(row["CarrierDelay"])
                weatherdel = int(row["WeatherDelay"])
                NASdel = int(row["NASDelay"])
                securitydel = int(row["SecurityDelay"])
                lateACdel = int(row["LateAircraftDelay"])
                
                tailnum = row["TailNum"]
                mfryear = planeDict.read(tailnum)

                flightnum = int(row["FlightNum"])
            
            except:
                continue

            else:
                yield Flight(year, month, day, dow, 
                dep_hour, dep_min, arr_hour, arr_min,
                depdelay, arrdelay, 
                carrierdel, weatherdel, NASdel, 
                securitydel, lateACdel, 
                tailnum, mfryear, flightnum)



## READ CSV plane-data

class KVStore:
    """ K/V store with CRUD"""

    def __init__(self):
        self._content = {}

    def create(self, key, val):
        """ Insert an entry.

        Parameters
        ------------
        key:
                key of the new entry.
        val:
                value of the new entry.

        Raises
        ------------
        KeyAlreadyExists
                when the entered key already exists.

        """
        if key in self._content:
            raise KeyAlreadyExists

        self._content[key] = val

    def read(self, key):
        """ Reads an entry.

        Parameters
        ------------
        key:
                key of the entry to read.

        Raises
        ------------
        MissingKeyError
                when the key is missing.

        """

        if key not in self._content:
            raise MissingKeyError

        return self._content[key]

    def update(self, key, val):
        """ Updates an entry.

        Parameters
        ------------
        key:
                key of the entry to update.
        val:
                new value of the entry to update.

        Raises
        ------------
        MissingKeyError
                     when key is missing.
        """
        if key not in self._content:
            raise MissingKeyError

        self._content[key] = val

    def delete(self, key):
        """ Deletes an entry.

        Parameters
        ------------
        key:
                key of the entry to delete.

        Raises
        ------------
        MissingKeyError
                when the key is missing.

        """

        if key not in self._content:
            raise MissingKeyError

        del self._content[key]

def createPlaneDict_from_csv(f):
    """ Creates dictionary with plane data from csv file.

    Parameters
    ------------
    f:
        name of csv file containing plane data (plane-data.csv)

    """

    MFRYear = KVStore()

    for row in csv.DictReader(open(f)):
        if (type(row['year']) == str) & (row['year'] != 'None') & (row['year'] is not None):
            MFRYear.create(row['tailnum'], int(row['year']))
    return MFRYear

