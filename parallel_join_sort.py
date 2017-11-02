#!/usr/bin/python2.7
#
# This project done as part of CSE 512 Fall 2017 
#
__author__ = "Prashant Gonarkar"
__version__ = "v0.1"
__email__ = "pgonarka@asu.edu"

import psycopg2
import os
import sys
import threading

TOTAL_THREADS = 5
RANGE_PARTITION = "rangeparition"
JOIN_RANGE_PARTITION = "joinrangepartition"
TABLE1_RANGE_PARTITION = "table1_rangeparition"
TABLE2_RANGE_PARTITION = "table2_rangeparition"

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    cur = openconnection.cursor()
    
    #STEP-1 Range Partition
    # Find min in SortingColumn
    query = "select  min({0}) from {1} ".format(SortingColumnName, InputTable)
    cur.execute(query)
    lowerbound = cur.fetchone()[0]

    # Find max in SortingColumn
    query = "select  max({0}) from {1} ".format(SortingColumnName, InputTable)
    cur.execute(query)
    upperbound = cur.fetchone()[0]

    partitioninterval = abs(upperbound - lowerbound) / float(TOTAL_THREADS)

    #print(upperbound,lowerbound,partitioninterval)

    # Creating tables for range partition step
    # There would be one table per thread to work upon
    for i in range(TOTAL_THREADS):
        outputtablename = RANGE_PARTITION + repr(i);
        copytable(InputTable, outputtablename , cur)

    #STEP-2 Parallel sorting 

    # Creating output table
    copytable(InputTable, OutputTable, cur)

    # thread pool for performing parallel sort
    threadspool = range(TOTAL_THREADS);

    for i in range(TOTAL_THREADS):
        if i == 0:
            start = lowerbound
            end = lowerbound + partitioninterval
        else:
            start = end
            end = end + partitioninterval
        rangetable = RANGE_PARTITION + repr(i)
        #print(i,lowerbound,upperbound)
        threadspool[i] = threading.Thread(target = parallelsorting, args = (InputTable, rangetable, 
                                SortingColumnName,  start, end, openconnection))

        threadspool[i].start()

    # wait to finish all threads
    for i in range(TOTAL_THREADS):
        threadspool[i].join()

    # Combine the result in output table
    for i in range(TOTAL_THREADS):
        tablename = RANGE_PARTITION + repr(i)
        query = "INSERT INTO {0} SELECT * FROM {1}".format( OutputTable, tablename)
        cur.execute(query)
        
    # delete all temp stuff
    for i in range(TOTAL_THREADS):
        tablename = RANGE_PARTITION + repr(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(tablename))
    
    openconnection.commit()



def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    cur = openconnection.cursor()

    #STEP-1 RangePartition

    # Find min and max in input table 1
    query = "select  min({0}) from {1} ".format(Table1JoinColumn, InputTable1)
    cur.execute(query)
    table1min = cur.fetchone()[0]
    
    query = "select  max({0}) from {1} ".format(Table1JoinColumn, InputTable1)
    cur.execute(query)
    table1max = cur.fetchone()[0]

    # Find max and max of input table 2 
    query = "select  min({0}) from {1} ".format(Table2JoinColumn, InputTable2)
    cur.execute(query)
    table2min = cur.fetchone()[0]

    query = "select  max({0}) from {1} ".format(Table2JoinColumn, InputTable2)
    cur.execute(query)
    table2max = cur.fetchone()[0]


    allmin = min(table1min,table2min)
    allmax = max(table1max,table2max)
    
    partitioninterval = abs(allmax - allmin) / float(TOTAL_THREADS) 
    #print(table1min,table1max,table2min,table2max,partitioninterval)
    rangepartitioning(InputTable1, Table1JoinColumn, partitioninterval, allmin, allmax, TABLE1_RANGE_PARTITION , cur)
    rangepartitioning(InputTable2, Table2JoinColumn, partitioninterval, allmin, allmax, TABLE2_RANGE_PARTITION, cur)


    #STEP-2 Parallel Join

    #Create temp range join tables for each thread

    for i in range(TOTAL_THREADS):
        outputtablename = JOIN_RANGE_PARTITION + repr(i)
        createjoinrangetable(InputTable1, InputTable2, outputtablename, cur)

    # thread pool for performing parallel sort
    threadspool = range(TOTAL_THREADS);

    for i in range(TOTAL_THREADS):
        inputtable1 = TABLE1_RANGE_PARTITION + repr(i)
        inputtable2 = TABLE2_RANGE_PARTITION + repr(i)
        outputtable = JOIN_RANGE_PARTITION + repr(i)

        threadspool[i] = threading.Thread(target = paralleljoin, args = (inputtable1, inputtable2, Table1JoinColumn, Table2JoinColumn, 
                                outputtable, openconnection))

        threadspool[i].start()

    # wait to finish all threads
    for i in range(TOTAL_THREADS):
        threadspool[i].join()

    # Create output table 
    createjoinrangetable(InputTable1, InputTable2, OutputTable, cur)

    # insert all results in the output table
    for i in range(TOTAL_THREADS):
        tablename = JOIN_RANGE_PARTITION + repr(i)
        query = "INSERT INTO {0} SELECT * FROM {1}".format(OutputTable,tablename)
        cur.execute(query)
    
    # Delete all intermediate partitions
    for i in range(TOTAL_THREADS):
        table1 = TABLE1_RANGE_PARTITION + repr(i)
        table2 = TABLE2_RANGE_PARTITION + repr(i)
        table3 = JOIN_RANGE_PARTITION + repr(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(table1))
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(table2))
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(table3))

    openconnection.commit()

    
####Support functions#####

def copytable(sourcetable,destinationtable,cur):

    query = "CREATE TABLE {0} AS SELECT * FROM {1} WHERE 1=2".format(destinationtable,sourcetable)
    cur.execute(query);

def parallelsorting(InputTable, rangetable, SortingColumnName, lowerbound, upperbound, openconnection): 

    cur = openconnection.cursor();
    if rangetable == 'rangeparition0':
        query = "INSERT INTO {0} SELECT * FROM {1}  WHERE {2} >= {3}  AND {2} <= {4} ORDER BY {2} ASC".format(rangetable,InputTable,
                                                                                                        SortingColumnName,lowerbound,upperbound)
    else:
        query = "INSERT INTO {0}  SELECT * FROM {1}  WHERE {2}  > {3}  AND {2}  <= {4} ORDER BY {2} ASC".format(rangetable,InputTable,  
                                                                                            SortingColumnName,lowerbound,upperbound)
    cur.execute(query)

def rangepartitioning(inputtable, tablejoincolumn, partitioninterval, allmin, allmax, tableprefix, cur):

    for i in range(TOTAL_THREADS):
        tablename = tableprefix + repr(i)
        
        if i == 0:
            lowerbound = allmin
            upperbound = lowerbound + partitioninterval
            #print(tablename,lowerbound,upperbound)
            query = "CREATE TABLE {0} AS  SELECT * FROM {1}  WHERE {2} >= {3} AND {2} <= {4};".format(tablename,inputtable,
                                                                                            tablejoincolumn,lowerbound,upperbound)
        else:
            lowerbound = upperbound
            upperbound = lowerbound + partitioninterval
            #print(tablename,lowerbound,upperbound)
            query = "CREATE TABLE {0} AS  SELECT * FROM {1}  WHERE {2} > {3} AND {2} <= {4};".format(tablename,inputtable,
                                                                                            tablejoincolumn,lowerbound,upperbound)

        cur.execute(query)

def createjoinrangetable(inputtable1, inputtable2, outputtablename, cur):
    query = "CREATE TABLE {0} AS SELECT * FROM {1},{2} WHERE 1=2".format(outputtablename, inputtable1, inputtable2)
    cur.execute(query);

def paralleljoin(inputtable1, inputtable2, Table1JoinColumn, Table2JoinColumn, outputtable, openconnection):
    cur = openconnection.cursor()
    query = "insert into {0} select * from {1} INNER JOIN {2} ON {1}.{3} = {2}.{4}".format(outputtable,
                                                            inputtable1,inputtable2,Table1JoinColumn,Table2JoinColumn)
    cur.execute(query) 
    

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
