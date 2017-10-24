# distributed-database
The primary goal of the project is to implement some of key concepts in distributed and parallel databases systems. For example operations like fragmentation, parallel sort, range query etc. This project is done as part of CSE 512 Distributed and Parallel Database Systems taught by [Mohamed Sarwat](http://faculty.engineering.asu.edu/sarwat/)

These concepts are built upon open source relational database [postgres](https://www.postgresql.org/). I have used [python](https://www.python.org/) for programming and psycopg as database driver for postgres. You can find getteting started guide for psycopg [here](http://prashant47.github.io/2017/Sep/20/psycopg_postgresql_adapter_for_python.html). 
 
The project covers 3 mains concepts
1. Data fragmentation acorss partitions.
2. Query processor that accesses data from the partitioned table.
3. Parallel sort and parallel join algorithm.

## Data Fragmentation
In centralized database sysytems, all the data is present in single node whereas in distributed and parallel database systems data is paritioned into multiple nodes. 
