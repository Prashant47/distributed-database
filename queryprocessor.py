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

RANGE_RATINGS_METADATA = 'rangeratingsmetadata'
ROUND_ROBIN_RATINGS_METADATA = 'roundrobinratingsmetadata'

RANGE_PARTITION_PREFIX = 'rangeratingspart'
ROUND_ROBIN_PARTITION_PREFIX = 'roundrobinratingspart'

RANGE_PARTITION_OUTPUT_NAME = 'RangeRatingsPart'
ROUND_ROBIN_PARTITION_OUTPUT_NAME = 'RoundRobinRatingsPart'

RANGE_QUERY_OUTPUT_FILE = 'RangeQueryOut.txt'
POINT_QUERY_OUTPUT_FILE = 'PointQueryOut.txt'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    
    #
    # Range query on range partitions

    try:
        cur = openconnection.cursor()

        # finding min boundary range of partition from metadata for given ratingMinValue 
        query = "select  max(minrating) from {0} where minrating <= {1}".format(RANGE_RATINGS_METADATA,ratingMinValue)	
        cur.execute(query)
        minpartboundary = cur.fetchone()[0]

        # finding min boundary range of partition from metadata for given ratingMinValue
        query = "select  min(maxrating) from {0} where maxrating >= {1}".format(RANGE_RATINGS_METADATA,ratingMaxValue)
        cur.execute(query)
        maxpartboundary = cur.fetchone()[0]

        # calculating the paratitions from metadata table where tuples of  given ranges lies 
        query = "select  partitionnum from {0} where maxrating >= {1} and maxrating <= {2}".format(RANGE_RATINGS_METADATA,minpartboundary,maxpartboundary)
        cur.execute(query)
        rows = cur.fetchall()

        if os.path.exists(RANGE_QUERY_OUTPUT_FILE):
            os.remove(RANGE_QUERY_OUTPUT_FILE)

        for i in rows:
            partitionname = RANGE_PARTITION_OUTPUT_NAME + repr(i[0])
            query = "select * from {0} where rating >= {1} and rating <= {2}".format(partitionname, ratingMinValue, ratingMaxValue) 
            cur.execute(query)
            rows2 = cur.fetchall()
            with open(RANGE_QUERY_OUTPUT_FILE,'a+') as f:
                for j in rows2:
                    f.write("%s," % partitionname)
                    f.write("%s," % str(j[0]))
                    f.write("%s," % str(j[1]))
                    f.write("%s\n" % str(j[2]))
        #
        # Range query on round robin paritions

        # get no of round robin partitions
        query = "select partitionnum from {0} ".format(ROUND_ROBIN_RATINGS_METADATA)	
        cur.execute(query)
        rrpartitioncount = int(cur.fetchone()[0])
        
        for i in range(rrpartitioncount):
            partitionname = ROUND_ROBIN_PARTITION_OUTPUT_NAME + repr(i)
            query = "select * from {0} where rating >= {1} and rating <= {2}".format(partitionname, ratingMinValue, ratingMaxValue) 
            cur.execute(query)
            rows2 = cur.fetchall()
            with open(RANGE_QUERY_OUTPUT_FILE,'a+') as f:
                for j in rows2:
                    f.write("%s," % partitionname)
                    f.write("%s," % str(j[0]))
                    f.write("%s," % str(j[1]))
                    f.write("%s\n" % str(j[2]))

    except Exception as ex:
        print("Exception while processing RangeQuery: ",ex)


def PointQuery(ratingsTableName, ratingValue, openconnection):

    # Point query for range partition 
    try:

        cur = openconnection.cursor()
        if ratingValue == 0:
            rangepartitionnum = 0
        else:
            query = "select partitionnum from {0} where minrating < {1} and maxrating >= {1}".format(RANGE_RATINGS_METADATA,ratingValue)
            cur.execute(query)
            rangepartitionnum = cur.fetchone()[0]

        partitionname = RANGE_PARTITION_OUTPUT_NAME + repr(rangepartitionnum)
        query = "select * from {0} where rating = {1} ".format(partitionname, ratingValue ) 
        cur.execute(query)
        rows2 = cur.fetchall()

        if os.path.exists(POINT_QUERY_OUTPUT_FILE):
            os.remove(POINT_QUERY_OUTPUT_FILE)

        with open(POINT_QUERY_OUTPUT_FILE,'a+') as f:
            for j in rows2:
                f.write("%s," % partitionname)
                f.write("%s," % str(j[0]))
                f.write("%s," % str(j[1]))
                f.write("%s\n" % str(j[2]))

        #
        # Point query for round robin partition
        query = "select partitionnum from {0} ".format(ROUND_ROBIN_RATINGS_METADATA)	
        cur.execute(query)
        rrpartitioncount = int(cur.fetchone()[0])
        
        for i in range(rrpartitioncount):
            partitionname = ROUND_ROBIN_PARTITION_OUTPUT_NAME + repr(i)
            query = "select * from {0} where rating = {1} ".format(partitionname, ratingValue ) 
            cur.execute(query)
            rows2 = cur.fetchall()
            with open(POINT_QUERY_OUTPUT_FILE,'a+') as f:
                for j in rows2:
                    f.write("%s," % partitionname)
                    f.write("%s," % str(j[0]))
                    f.write("%s," % str(j[1]))
                    f.write("%s\n" % str(j[2]))

    except Exception as ex:
        print("Exception while processing RangeQuery: ",ex)
