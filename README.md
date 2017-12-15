# Simplified-Query-Processor

PARTITIONING THE TABLE:

1. ImplementaPythonfunctionLoadRatings()thattakesafilesystempaththatcontainstherating.dat file as input. Load Ratings() then      load the rating.dat content into a table (saved in PostgreSQL) named Ratings that has the following schema
                                          UserID - MovieID - Rating

2. Implement a Python function Range Partition() that takes as input: (1) the Ratings table stored in PostgreSQL and (2) an      integer value N; that represents the number of partitions. Range Partition() then generates N horizontal fragments of the      Ratings table and store them in PostgreSQL. The algo- rithm should partition the ratings table based on N uniform ranges of    the Rating attribute.

3. Implement a Python function RoundRobin Partition() that takes as input: (1) the Ratings table stored in PostgreSQL and (2)    an integer value N; that represents the number of partitions. The function then generates N horizontal fragments of the        Ratings table and stores them in PostgreSQL. The algorithm should partition the ratings table using the round robin            partitioning approach

4. Implement a Python function RoundRobin Insert() that takes as input: (1) Ratings table stored in PostgreSQL, (2) UserID,      (3) ItemID, (4) Rating. RoundRobin Insert() then inserts a new tuple in the right fragment (of the partitioned ratings        table) based on the round robin approach.

5. Implement a Python function Range Insert() that takes as input: (1) Ratings table stored in Post- greSQL (2) UserID, (3)      ItemID, (4) Rating. Range Insert() then inserts a new tuple in the correct fragment (of the partitioned ratings table)        based upon the Rating value.

6. Implement a Python function Delete Partitions() that deletes all generated partitions as well as any metadata related to      the partitioning scheme.

NOTE: Download rating.dat file from the MovieLens website (http://files.grouplens.org/datasets/movielens/ml- 10m.zip)
                                           
**PARALLEL SORT:**

Implemented a Python function ParallelSort() that takes as input: (1) InputTable stored in a PostgreSQL database, (2) SortingColumnName the name of the column used to order the tuples by. ParallelSort() then sorts all tuples (using five parallelized threads) and stores the sorted tuples for in a table named OutputTable (the output table name is passed to the function). The OutputTable contains all the tuple present in InputTable sorted in ascending order.

**PARALLEL JOIN:**

Implement a Python function ParallelJoin() that takes as input: (1) InputTable1 and InputTable2 table stored in a PostgreSQL database, (2) Table1JoinColumn and Table2JoinColumn that represent the join key in each input table respectively. ParallelJoin() then joins both InputTable1 and InputTable2 (using five parallelized threads) and stored the resulting joined tuples in a table named OutputTable (the output table name is passed to the function). The schema of OutputTable should be similar to the schema of both InputTable1 and InputTable2 combined.

**Function Interface: -**

**ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn,OutputTable, openconnection)**

**InputTable1** – Name of the first table on which you need to perform join.
**InputTable2** – Name of the second table on which you need to perform join.
**Table1JoinColumn** – Name of the column from first table i.e. join key for first table.
**Table2JoinColumn** – Name of the column from second table i.e. join key for second table.
**OutputTable** - Name of the table where the output needs to be stored.
**openconnection** – connection to the database.
                                                                 
                                                                
