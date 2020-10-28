import csv
import json
import datetime
import functools
import collections
import numpy as np
import cassandra
import cassandra.cluster
import feed_cassandra
import flight_data
import matplotlib.pyplot as plt


def _corr_mapping(flight):
    """ Returns from a flight's data a tuple containing the values needed to calculate Pearson's empirical correlation of departure hour and delay time. 

    Parameters
    ------------
        flight:
                tuple containing the following field values of a flight: 
                "Year", "Month", "Day", "DayOfWeek", 
                "CRSDepHour", "CRSDepMin", "CRSArrHour", "CRSArrMin",
                "DepDelay", "ArrDelay", "CarrierDelay", "WeatherDelay", "NASDelay", 
                "SecurityDelay", "LateAircraftDelay", 
                "TailNum", "MFRYear","FlightNum"

    """
    x = flight.CRSDepHour
    y = flight.DepDelay
    return (1, x, y, x * y, x ** 2, y ** 2)


def addtuple(x, y):
    """ Sums elements of 2 iterables by pairs of values. Returns a tuple containing the results. (Reducer)

    Parameters
    ------------
        x: 
            first iterable.
        y:
            second iterable.

    """
    return tuple(a + b for (a, b) in zip(x, y))


def corr_emp(stream):
    
    """ Returns Pearson's empirical correlation of departure hour and delay time from a flight data generator (using Map/Reduce). 

    Parameters
    ------------
        stream:
                stream of flight data to use to calculate the correlation coefficient. (flight data generator) 

    """
    mapped_stream = map(_corr_mapping, stream)
    
    f0 = flight_data.Flight(0, 0, 0, 0, 
                0, 0, 0, 0,  0, 0, 
                0, 0, 0, 0, 0, 
                0, 0, 0)   #initializer

    (s1, sx, sy, sxy, sx2, sy2) = functools.reduce(addtuple, mapped_stream, f0)

    mean_x = sx / s1
    mean_y = sy / s1
    var_x = sx2 / s1 - mean_x ** 2
    var_y = sy2 / s1 - mean_y ** 2
    covar_xy = sxy / s1 - mean_x * mean_y
    corr = covar_xy / np.sqrt(var_x * var_y)

    return corr


def _meanvar_bydow_mapping(flight):
    
    """ Returns from a flight's data a tuple containing the values needed to compute mean & variance values of flight delay time. 

    Parameters
    ------------
        flight:
                tuple containing the following field values of a flight: 
                "Year", "Month", "Day", "DayOfWeek", 
                "CRSDepHour", "CRSDepMin", "CRSArrHour", "CRSArrMin",
                "DepDelay", "ArrDelay", "CarrierDelay", "WeatherDelay", "NASDelay", 
                "SecurityDelay", "LateAircraftDelay", 
                "TailNum", "MFRYear","FlightNum"

    """
    x = flight.DepDelay
    return (1, x, x ** 2)



def meanvar_bydow(stream):
    
    """ Returns mean and variance values of departure delay time from a flight data generator (using Map/Reduce). 

    Parameters
    ------------
        stream:
                stream of flight data to use to calculate mean & variance. (flight data generator) 

    """

    mapped_stream = map(_meanvar_bydow_mapping, stream)
    
    f0 = flight_data.Flight(0, 0, 0, 0, 
                0, 0, 0, 0,  0, 0, 
                0, 0, 0, 0, 0, 
                0, 0, 0)   #initializer

    (s1, sx, sx2) = functools.reduce(addtuple, mapped_stream, [0,0,0])

    mean = sx / s1
    var = sx2 / s1 - mean ** 2

    return mean, var



def _meanvar_bymonth_mapping(flight):
    
    """ Returns from a flight's data a tuple containing the values needed to compute mean & variance values of general & weather-related flight delay time. 

    Parameters
    ------------
        flight:
                tuple containing the following field values of a flight: 
                "Year", "Month", "Day", "DayOfWeek", 
                "CRSDepHour", "CRSDepMin", "CRSArrHour", "CRSArrMin",
                "DepDelay", "ArrDelay", "CarrierDelay", "WeatherDelay", "NASDelay", 
                "SecurityDelay", "LateAircraftDelay", 
                "TailNum", "MFRYear","FlightNum"

    """

    x = flight.DepDelay
    y = flight.WeatherDelay
    return (1, x, x ** 2, y, y**2)



def meanvar_bymonth(stream):
    
    """ Returns mean and variance values of departure delay time (general & weather-related) from a flight data generator (using Map/Reduce). 

    Parameters
    ------------
        stream:
                stream of flight data to use to calculate mean & variance. (flight data generator) 

    """

    mapped_stream = map(_meanvar_bymonth_mapping, stream)
    
    f0 = flight_data.Flight(0, 0, 0, 0, 
                0, 0, 0, 0,  0, 0, 
                0, 0, 0, 0, 0, 
                0, 0, 0)   #initializer

    (s1, sx, sx2, sy, sy2) = functools.reduce(addtuple, mapped_stream, f0)

    mean_x = sx / s1
    var_x = sx2 / s1 - mean_x ** 2

    mean_y = sy / s1
    var_y = sy2 / s1 - mean_y ** 2

    return mean_x, var_x, mean_y, var_y