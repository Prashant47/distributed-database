# distributed-database
The primary goal of the project is to implement some of key concepts in distributed and parallel databases systems. For example operations like fragmentation, parallel sort, range query etc. This project is done as part of CSE 512 Distributed and Parallel Database Systems taught by [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/)

These concepts are built upon open source relational database [postgres](https://www.postgresql.org/). I have used [python](https://www.python.org/) for programming and psycopg as database driver for postgres. You can find getteting started guide for psycopg [here](http://prashant47.github.io/2017/Sep/20/psycopg_postgresql_adapter_for_python.html). 
 
The project covers 3 mains concepts
1. Data fragmentation acorss partitions. (Sharding)
2. Query processor that accesses data from the partitioned table.
3. Parallel sort and parallel join algorithm.

## Data Fragmentation
In centralized database sysytems, all the data is present in single node whereas in distributed and parallel database systems data is paritioned into multiple nodes. 

## Query Processor
It involves building a simplified query processor that accesses data from the partitioned table. As part of this two queries were implemented RangeQuery() and PointQuery(). 
</br>
RangeQuery() takes input as range of attribute and returns the tuples that come along with given range from fragmented partitions done in first step. 
</br>
PointQuery() takes input as specific value of attribute and returns all the tuples having the same value of attribute from gragmented paritions.


## Parallel Sort & Join
This task involves implementation generic parallel sort and join algorithm.




## Contribution
In case you like this utility or you find fun working with this project then feel free to contribute. For contributing you just need working knowledge of python, postgres & bit about distributed database concepts.
</br>
Some initial ideas would be adding few more queries in query processor .! 


## Issues

If you find any issue, bug, error or any unhandles exception, feel free to [report one](https://github.com/Prashant47/distributed-database/issues/new)
