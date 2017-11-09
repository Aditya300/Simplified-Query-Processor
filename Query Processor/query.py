#!/usr/bin/python2.7
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    #Remove this once you are done with implementation

    cursor=openconnection.cursor()
    new_list=[]

    cursor.execute("select count(*) from RangeRatingsMetaData;")
    rangecount = int(cursor.fetchone()[0])

    for i in range(rangecount):
        new_list.append("SELECT 'rangeratingspart" + str(i) + "' AS tablename, userid, movieid, rating FROM rangeratingspart" + str(i) +
                        " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))

    cursor.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    roundpartitions = int(cursor.fetchone()[0])

    for i in range(roundpartitions):
        new_list.append("SELECT 'roundrobinratingspart" + str(i) + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart" + str(i) +
                        " WHERE rating >= " + str(ratingMinValue) + " AND rating <= " + str(ratingMaxValue))

    op_query = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(new_list))
    print(op_query)
    opfile = open('RangeQueryOut.txt', 'w')

    write_file = "COPY (" + op_query + ") TO '" + os.path.abspath(opfile.name) + "' (FORMAT text, DELIMITER ',')"

    cursor.execute(write_file)

    cursor.close()
    opfile.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    # Remove this once you are done with implementation
    cursor = openconnection.cursor()
    new_list = []

    # Get range partition
    cursor.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    rangecount = int(cursor.fetchone()[0])

    # query for range
    for i in range(rangecount):
        new_list.append("SELECT 'rangeratingspart" + str(i) + "' AS tablename, userid, movieid, rating FROM rangeratingspart"
                        + str(i) + " WHERE rating = " + str(ratingValue))

    # Get round robin 
    cursor.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    roundnpartitions = int(cursor.fetchone()[0])

    # query for round
    for i in range(roundnpartitions):
        new_list.append("SELECT 'roundrobinratingspart" + str(i) + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart"
                        + str(i) + " WHERE rating = " + str(ratingValue))

    op_query = 'SELECT * FROM ({0}) AS T'.format(' UNION ALL '.join(new_list))
    opfile = open('PointQueryOut.txt', 'w')

    write_file = "COPY (" + op_query + ") TO '" + os.path.abspath(opfile.name) + "' (FORMAT text, DELIMITER ',')"

    cursor.execute(write_file)



    cursor.close()
    opfile.close()

