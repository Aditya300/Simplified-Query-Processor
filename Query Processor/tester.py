#!/usr/bin/python2.7
#
#Tester
# Do not hard code values in your program for ratings.
# table name and input file name.
# Do not close con objects in your program.
# Invalid ranges will not be tested.
# Order of output does not matter, only correctness will be checked.
# Use discussion board extensively to clear doubts.
# Sample output does not correspond to data in test_data.dat.
#

import partition as partition
import query as code

if __name__ == '__main__':
    try:
        #Creating Database ddsquery
        print "Creating Database"
        partition.createDB();

        # Getting connection to the database
        print "Getting connection from the database"
        con = partition.getOpenConnection();

        # Loading Ratings table
        print "Creating and Loading the ratings table"
        partition.loadRatings('ratings', '/filepath of dataset', con);

        # Doing Range Partition
        print "Doing the Range Partitions"
        partition.rangePartition('ratings', 5, con);

        # Doing Round Robin Partition
        print "Doing the Round Robin Partitions"
        partition.roundRobinPartition('ratings', 5, con);

        # Deleting Ratings Table because Point Query and Range Query should not use ratings table instead they should use partitions.
        partition.deleteTables('ratings', con);

        # Calling RangeQuery
        print "Performing Range Query"
        code.RangeQuery('ratings', 1.5, 3.5, con);
        #query.RangeQuery('ratings',1,4,con);

        # Calling PointQuery
        print "Performing Point Query"
        code.PointQuery('ratings', 4.5, con);
        #query.PointQuery('ratings',2,con);
        
        # Deleting All Tables
        partition.deleteTables('all', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
