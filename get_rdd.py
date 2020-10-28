import csv
import datetime
import itertools
import collections
import pyspark
import flight_data as fd

def read_flight_csvs(files, planeDict, limit = None):
    """Creates a stream of flight data from one or multiple flight csv files.
    
    Parameters
    ------------
    files:
            names of CSV files to read (list).

    planeDict:
            dictionary containing additional Plane data.
    
    limit:
            max number of generated elements in the stream.

    """

    gen = itertools.chain(*[fd.read_flight_csv(f, planeDict) for f in files])
    if limit is None:
        return gen
    return fd.limiter(gen, limit)

def get_flight_RDD(files, planeDict, sc = None, limit = None, numSlices = None):
    
    """Creates a RDD of flight data from one or multiple flight csv files.
    
    Parameters
    ------------
    files:
            names of CSV files to read (list).

    planeDict:
            dictionary containing additional Plane data.

    sc: 
            SparkConf object if already created. If not (value = None), a new SparkConf object is created.
    
    limit:
            max number of generated elements in the stream.
    
    numSlices:
            number of partitions to cut the dataset into.

    """

    if sc is None:
          sparkconf = pyspark.SparkConf()
          sparkconf.set('spark.port.maxRetries', 128)
          sc = pyspark.SparkContext(conf = sparkconf)
    if numSlices is None:
          numSlices = 1000

    return sc, sc.parallelize(read_flight_csvs(files, planeDict, limit), numSlices = numSlices)

