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
                                           
                                                                      
                                                                 
