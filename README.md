# Airline-on-time-performance-Big-Data

The scripts in this repository contain functions developed in the context of a mini-project completed for Big Data course. The mission is to use different Big Data analysis tools on ASA's [2009 Airline on-time performance dataset](http://stat-computing.org/dataexpo/2009/). 

## Part 1 : Data analysis using a column-oriented DBMS

The first part of the project requires a column-oriented storage of data & data extraction from the created columnar database.

The chosen mission is to tackle the following question : 
* When is the best time of day/day of week/time of year to fly to minimise delays ? 

The functions that store data from a csv file into the CQL column-oriented tables & query data are developed in *feed_cassandra.py*. 
*cassandra_table.cql* creates the column-oriented tables in Cassandra. *analyse_cassandra.py* comprises the data analysis functions. 
CSV file reading functions were developed in *flight_data.py*.

## Part 2 : Data analysis using Distributed/Parallel Computing

The second part of the project consists of implementing data analysis functions using parallel computing. 

This part tackles the following question : 
* Do older planes suffer more delays ?

Data is stored & distributed in RDD partitions (using Spark). *get_rdd.py* contains the RDD creation functions.
All data analysis is computed using MapReduce functions (*analyse_spark.py*). 

## Tech/framework used
<b>Built with</b>
- [Python](https://www.python.org/)
    - [PySpark API](https://spark.apache.org/docs/latest/api/python/index.html)
    
- [Apache Cassandra](https://cassandra.apache.org/)
- [Apache Spark](https://spark.apache.org/)
  
## Credits
The work on the mini-project was done by Rami Jerbaka & guided by the Big Data course teacher, Jean-Benoist Leger. 