#!/usr/bin/python2.7

#

import psycopg2

DATABASE_NAME = 'db_name' #your db name

#provide username, pwd, dbname

def getopenconnection(user='', password='', dbname=''):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath , openconnection):
    cur=openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS "+ratingstablename+" ;" )
    cur.execute("CREATE TABLE " +ratingstablename+ " (rowID serial primary key, UserID int, delimiter1 varchar, MovieID int, delimiter2 varchar, Rating real, delimiter3 varchar, total_seconds int);")
    input_file=open(ratingsfilepath)
    cur.copy_from(input_file,ratingstablename, sep=':', columns=('UserID','delimiter1','MovieID','delimiter2','Rating','delimiter3','total_seconds'))
    cur.execute("ALTER TABLE Ratings DROP COLUMN delimiter1, DROP COLUMN delimiter2, DROP COLUMN delimiter3,DROP COLUMN total_seconds;")
    cur.close()

def range_partition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    global range_condition  # declared globally, so that it can be accessed in rangeinsert() function
    global x
    x = int(numberofpartitions)
    range_condition = float(5.0 / numberofpartitions)

    for i in range(0, x):
        if i == 0:
            j = float(i)
            cur.execute("DROP TABLE IF EXISTS range_part" + str(i) + ";")
            cur.execute(
                "CREATE TABLE range_part" + str(i) + " AS SELECT * FROM " + ratingstablename + " WHERE Rating >=" + str(
                    j * range_condition) +
                " AND Rating <=" + str((j + 1) * range_condition) + " ;")
        else:
            j = float(i)
            cur.execute("DROP TABLE IF EXISTS range_part" + str(i) + ";")
            cur.execute(
                "CREATE TABLE range_part" + str(i) + " AS SELECT * FROM " + ratingstablename + " WHERE Rating >" + str(
                    j * range_condition) +
                " AND Rating <=" + str((j + 1) * range_condition) + " ;")

    cur.close()



def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):

    cur = openconnection.cursor()

    partition_list = list(reversed(range(numberofpartitions)))
    global nofp
    nofp = numberofpartitions
    global last_insert_position
    last_insert_position = 0
    j = 0

    for i in partition_list:
        cur.execute("DROP TABLE IF EXISTS rrobin_part" + str(i))
        cur.execute("CREATE TABLE rrobin_part"+str(i)+" AS SELECT UserID, MovieID, Rating FROM "+ratingstablename+" WHERE rowid % "
                        +str(numberofpartitions)+" = " + str((i + 1) % numberofpartitions))
        p_rowno = cur.execute("SELECT COUNT (*) FROM rrobin_part" +str(i)+ " ;")

        if p_rowno > j:                     #used in roundrobininsert() function
            last_insert_position = i
            j = rowNo_partition

    cur.close()



def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    cur=openconnection.cursor()
    global last_insert_position
    global nofp
    p_end = last_insert_position % nofp
    cur.execute("INSERT INTO rrobin_part" +str(p_end)+ " (UserID,MovieID,Rating) VALUES (" +str(userid)+ "," +str(
            itemid)+ "," +str(rating)+ ");")
    cur.close()



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur=openconnection.cursor()
    new_range=5.0/x

    low_bound=0
    cur_partition=0
    up_bound=new_range

    while low_bound<5:
        if low_bound ==0:
            if rating >= low_bound and rating <= up_bound:
                break
            cur_partition=cur_partition+1
            low_bound=low_bound+new_range
            up_bound=up_bound+new_range

        else:
            if rating > low_bound and rating <= up_bound:
                break
            cur_partition = cur_partition + 1
            low_bound = low_bound + new_range
            up_bound = up_bound + new_range

    cur.execute("INSERT INTO range_part" +str(cur_partition)+ "(UserID, MovieID, rating) VALUES ("  + str(userid) + "," + str(
            itemid) + "," + str(rating) + ")")

    cur.close()

def delete_partitions(openconnection):
    cur = openconnection.cursor()

    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format("range_part"))
    count2 = int(cur.fetchone()[0])

    if count2 != 0:
        for p in range(0,count2):
            cur.execute("DROP TABLE IF EXISTS " + "range_part" + str(p + 1) + ";")

    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '{0}%';".format("rrobin_part"))
    count2 = int(cur.fetchone()[0])

    if count2 != 0:
        for q in range(0,count2):
            cur.execute("DROP TABLE IF EXISTS " + "rrobin_part" + str(q + 1) + ";")

    cur.close()

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

            # Here is where I will start calling your functions to test them. For example

            loadratings(ratingstablename ='Ratings', ratingsfilepath = '/Users/athithyaaselvam/Desktop/test_data.dat',openconnection = con)
            # range_partition(ratingstablename='Ratings', numberofpartitions= 5, openconnection = con)
            #roundrobinpartition(ratingstablename='Ratings', numberofpartitions=5, openconnection=con)
            # roundrobininsert(ratingstablename='Ratings', userid=20, itemid=31, rating=2, openconnection=con)
            #rangeinsert(ratingstablename='Ratings', userid=45, itemid=17, rating=3.5, openconnection=con)
            #delete_partitions(con)

     

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
