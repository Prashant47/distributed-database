#!/usr/bin/python2.7
#
# This project done as part of CSE 512 Fall 2017
#

__author__ = "Prashant Gonarkar"
__version__ = "v0.1"
__email__ = "pgonarka@asu.edu"

import psycopg2
import csv
import os

DATABASE_NAME = 'dds_assgn2'
USER_ID_COLNAME = 'userId'
MOVIE_ID_COLNAME = 'movieId'
RATING_COLNAME = 'rating'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
RATINGS_TABLE = 'ratings'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):

    cur = openconnection.cursor()
    createtable(ratingstablename,cur)

    # For inserting data COPY method of postgres is used being the fastest among other approaches.
    # But input data has multi-byte characters as delimiters (::) which postgres doesn't support  
    # hence input data is preprocessed and converted into tab seprated data


    # Preprocessing the data before importing into postgres
    try:
    	with open(ratingsfilepath) as finput, open('/tmp/_output_zxyvk.csv','w') as foutput:
		csv_output = csv.writer(foutput,delimiter='\t')
		for lines in finput:
			line = lines.rstrip('\n')
			splits = line.split("::")
			#print("value of splits: ",splits)
			csv_output.writerow(splits[0:3])
    except Exception as ex:
	print("File processing error: ",ex)
    
    with open('/tmp/_output_zxyvk.csv') as datafile:
    	try: 
		cur.copy_from(datafile,ratingstablename )
		openconnection.commit()
		cur.close()
    	except Exception as ex:
    		print("Failed to copy file in database: ",ex)

    # removing temporary intermediate file created while data preprocessing
    os.remove('/tmp/_output_zxyvk.csv')    
    return

def rangepartition(ratingstablename, numberofpartitions, openconnection):

    # find max and min to get the range but here range is being given.
    # As given range is given as [0-5]
    ratinglowerbound = 0.0
    ratingupperbound = 5.0
    partitioninterval = abs(ratingupperbound-ratinglowerbound) / numberofpartitions

    cur = openconnection.cursor()
    for i in range( 0, numberofpartitions ):
    	partitiontablename = RANGE_TABLE_PREFIX + repr(i) 
	createtable(partitiontablename,cur)

	# upper and lower bounds for created partition table
	lowerbound = i * partitioninterval
	upperbound = lowerbound + partitioninterval
	
	# inserting values according to range in table
	if lowerbound == ratinglowerbound:
		query = " INSERT INTO {0} SELECT * FROM {1} WHERE {2} >= {3} and {2} <= {4}".format( partitiontablename,
                                                                                            ratingstablename,
                                                                                            RATING_COLNAME,
                                                                                            lowerbound,
                                                                                            upperbound )
	else:
		query = " INSERT INTO {0} SELECT * FROM {1} WHERE {2} > {3} and {2} <= {4}".format( partitiontablename,
                                                                                            ratingstablename,
                                                                                            RATING_COLNAME,
                                                                                            lowerbound,
                                                                                            upperbound )
	cur.execute(query)
	openconnection.commit()
    	print("Created partition table: ",partitiontablename)
    pass

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    
    # create round robin partitions
    modvalue = 0
    cur = openconnection.cursor()
    for i in range( 0, numberofpartitions ):
        partitiontablename = RROBIN_TABLE_PREFIX + repr(i)
        createtable(partitiontablename,cur)
	print("partitiontablename: ",partitiontablename)

    	# modvalue acts as partition selector in query 
    	# e.g for modvalue 1 with no of partitions 4, the query will select rows number 1, 5, 9, ..

	if (i != (numberofpartitions - 1)):
    		modvalue = i + 1;
	else:
		modvalue = 0;
	print("mod value ",modvalue)	
	
	try:
		query = "INSERT INTO {0} " \
		        "SELECT {1},{2},{3} " \
                        "FROM (SELECT ROW_NUMBER() OVER() as row_number,* FROM {4}) as foo " \
                        "WHERE MOD(row_number,{5}) = cast ('{6}' as bigint) ".format(partitiontablename,USER_ID_COLNAME, MOVIE_ID_COLNAME,
                                                                               RATING_COLNAME, ratingstablename, numberofpartitions, modvalue)

    		cur.execute(query)
		openconnection.commit()
	except Exception as ex:
		print(ex)
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    # Round Robin insert approach: start comparing count of adjacent two tables
    # if all tables have same count then insert into first table, and if next table
    # has less count than previous table then insert into next table
    #

    cur = openconnection.cursor()
    # calculate number of partitions
    partitioncount = tablecount(cur, RROBIN_TABLE_PREFIX)
    print ('partition count ', partitioncount)

    partitiontoinsert = 0
    previouscount = countrowsintable(cur, RROBIN_TABLE_PREFIX + repr(0) )
    
    for i in range(1,partitioncount):
	nextcount = countrowsintable(cur, RROBIN_TABLE_PREFIX + repr(i) )
	if ( nextcount < previouscount ):
		partitiontoinsert = i
		break

    # inserting in ratings table 
    query = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format( ratingstablename,
                                                              userid, itemid, rating )
    cur.execute(query)

    # inserting in appropriate round robin partition 	
    query = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format( RROBIN_TABLE_PREFIX+repr(partitiontoinsert),
							    userid, itemid, rating )
    cur.execute(query)
    openconnection.commit()
    print("Inserted value in partition: ",RROBIN_TABLE_PREFIX+repr(partitiontoinsert)) 
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    
    cur = openconnection.cursor()
    # calculate number of partitions
    partitioncount = tablecount(cur, RANGE_TABLE_PREFIX)
    print ('partition count ', partitioncount )

    # As given range is given as [0-5]
    ratinglowerbound = 0.0
    ratingupperbound = 5.0
    partitioninterval = abs(ratingupperbound-ratinglowerbound) / partitioncount

    for i in range( 0, partitioncount):
	# upper and lower bounds for created partition table
	lowerbound = i * partitioninterval
	upperbound = lowerbound + partitioninterval

	if lowerbound == ratinglowerbound:
		if (rating >= lowerbound) and (rating <= upperbound):
			break
	elif (rating > lowerbound) and (rating <= upperbound):
		break
    partitiontoinsert = i

    # inserting in ratings table
    query = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format( ratingstablename,
                                                             userid, itemid, rating )
    cur.execute(query)

    # inserting in appropriate range partition table
    query = " INSERT INTO {0} VALUES ({1}, {2}, {3})".format( RANGE_TABLE_PREFIX + repr(partitiontoinsert),
							    userid, itemid, rating )
    cur.execute(query)
    openconnection.commit()
    print("Inserted value in partition: ", RANGE_TABLE_PREFIX + repr(partitiontoinsert)) 
    pass

def deletepartitionsandexit(openconnection):
    
    cur = openconnection.cursor()
    # delete range paritions
    partitioncount = tablecount(cur, RANGE_TABLE_PREFIX)
    print ('partition count %s', partitioncount)
    for i in range(0, partitioncount):
        partitionname = RANGE_TABLE_PREFIX + repr(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(partitionname))
        openconnection.commit()

    # delete round robin paritions
    partitioncount = tablecount(cur, RROBIN_TABLE_PREFIX)
    print ('partition count %s', partitioncount)
    for i in range(0, partitioncount):
        partitionname = RROBIN_TABLE_PREFIX + repr(i)
        cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(partitionname))
        openconnection.commit()

    # delete ratings parition
    cur.execute('DROP TABLE IF EXISTS {0} CASCADE'.format(RATINGS_TABLE))
    openconnection.commit()

def tablecount(cur, tableprefix):
    query = "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND                                                                         table_name LIKE '{0}%';".format(tableprefix)
    cur.execute(query)
    partitioncount = int(cur.fetchone()[0])
    return partitioncount

def countrowsintable( cur, tablename):
    
    query = "SELECT count(*) FROM {0}".format(tablename)
    cur.execute(query)
    count = int(cur.fetchone()[0])
    return count

def createtable(tablename,cursor):

    try:
	query = "CREATE TABLE {0} ( {1} integer, {2} integer,{3} real);".format(tablename,
                                                        USER_ID_COLNAME,MOVIE_ID_COLNAME,RATING_COLNAME)
        cursor.execute(query)
    except Exception as ex: 
	print("Failed to create table: ",ex)
    print("Created table:  ",tablename) 


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
